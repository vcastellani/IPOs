"""
EDGAR EFFECT Filing Scraper
Fetches daily EFFECT filings (IPO/SEO/SPAC/REIT registrations becoming effective)
and emails a formatted summary.
"""

import os
import sys
import time
import smtplib
import logging
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{}.json"
EDGAR_FILING_BASE = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type=EFFECT&dateb=&owner=include&count=5"

# SEC requires a descriptive User-Agent
HEADERS = {
    "User-Agent": os.environ.get(
        "EDGAR_USER_AGENT", "IPOTracker/1.0 research@example.com"
    ),
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

# Registration form types -> offering category
FORM_CATEGORIES = {
    "S-1": "IPO",
    "S-1/A": "IPO",
    "F-1": "IPO (Foreign)",
    "F-1/A": "IPO (Foreign)",
    "S-11": "REIT IPO",
    "S-11/A": "REIT IPO",
    "S-3": "SEO / Shelf",
    "S-3/A": "SEO / Shelf",
    "S-3ASR": "SEO / Shelf",
    "S-4": "Merger / SPAC",
    "S-4/A": "Merger / SPAC",
    "F-3": "SEO / Shelf (Foreign)",
    "F-3/A": "SEO / Shelf (Foreign)",
    "F-4": "Merger (Foreign)",
    "F-4/A": "Merger (Foreign)",
}


def get_previous_business_day() -> date:
    today = date.today()
    if today.weekday() == 0:
        return today - timedelta(days=3)
    return today - timedelta(days=1)


def fetch_effect_filings(filing_date: date) -> list[dict]:
    date_str = filing_date.strftime("%Y-%m-%d")
    log.info("Fetching EFFECT filings for %s", date_str)

    filings: list[dict] = []
    from_idx = 0

    while True:
        resp = requests.get(
            EDGAR_SEARCH_URL,
            params={
                "forms": "EFFECT",
                "dateRange": "custom",
                "startdt": date_str,
                "enddt": date_str,
                "from": from_idx,
                "size": 100,
            },
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        filings.extend(hits)
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        from_idx += len(hits)
        log.info("  Retrieved %d / %d filings", from_idx, total)

        if from_idx >= total:
            break

        time.sleep(0.15)

    return filings


def get_company_info(cik: str) -> dict:
    url = EDGAR_SUBMISSIONS_URL.format(cik.zfill(10))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])

        first_filing_date = dates[-1] if dates else ""

        # Count prior EFFECT filings; <= 1 means this is the first
        # (0 if current filing hasn't propagated to history yet, 1 if it has)
        effect_count = forms.count("EFFECT")
        is_first_effect = effect_count <= 1

        category = "Other"
        for form in forms:
            if form == "EFFECT":
                continue
            cat = FORM_CATEGORIES.get(form)
            if cat:
                category = cat
                break
            if form.startswith("S-") or form.startswith("F-"):
                category = f"Other ({form})"
                break

        return {"first_filing_date": first_filing_date, "category": category, "is_first_effect": is_first_effect}
    except requests.HTTPError as exc:
        log.warning("HTTP error fetching submissions for CIK %s: %s", cik, exc)
        return {"first_filing_date": "", "category": "Unknown", "is_first_effect": False}
    except Exception as exc:
        log.warning("Error fetching submissions for CIK %s: %s", cik, exc)
        return {"first_filing_date": "", "category": "Unknown", "is_first_effect": False}
    finally:
        time.sleep(0.15)


def parse_filings(raw_hits: list[dict]) -> list[dict]:
    parsed = []
    for hit in raw_hits:
        src = hit.get("_source", {})

        # display_names format: "Company Name  (TICKER)  (CIK 0001234567)"
        display_names = src.get("display_names", [])
        company = display_names[0].split("(")[0].strip() if display_names else "N/A"

        # ciks is a list of zero-padded CIK strings
        ciks = src.get("ciks", [])
        cik = ciks[0].lstrip("0") if ciks else ""

        # accession number is in 'adsh' field
        accession = src.get("adsh", "")
        accession_path = accession.replace("-", "")

        parsed.append(
            {
                "company": company,
                "cik": cik,
                "accession": accession,
                "file_date": src.get("file_date", ""),
                "first_filing_date": "",
                "is_first_effect": False,
                "filing_url": (
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/"
                    if cik and accession_path
                    else "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=EFFECT"
                ),
                "edgar_url": EDGAR_FILING_BASE.format(cik) if cik else "",
            }
        )
    return parsed


CATEGORY_COLORS = {
    "IPO": "#1a7f3c",
    "IPO (Foreign)": "#2e8b57",
    "REIT IPO": "#4169e1",
    "SEO / Shelf": "#c07000",
    "SEO / Shelf (Foreign)": "#c07000",
    "Merger / SPAC": "#8b0000",
    "Merger (Foreign)": "#8b0000",
    "Other": "#555555",
    "Unknown": "#999999",
}


def build_html_email(filings: list[dict], filing_date: date) -> str:
    date_str = filing_date.strftime("%A, %B %-d, %Y")

    if not filings:
        body = "<p>No EFFECT filings were found on EDGAR for this date.</p>"
    else:
        rows = ""
        for f in filings:
            color = CATEGORY_COLORS.get(f["category"], "#555555")
            first_effect_cell = "Yes" if f["is_first_effect"] else "No"
            rows += f"""
            <tr>
              <td style="padding:8px 12px; border-bottom:1px solid #eee;">
                <a href="{f['edgar_url']}" style="color:#1a56db; text-decoration:none; font-weight:600;">
                  {f['company']}
                </a>
              </td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#555;">{f['cik']}</td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee;">
                <span style="background:{color}; color:#fff; padding:2px 8px; border-radius:4px;
                             font-size:12px; font-weight:600; white-space:nowrap;">
                  {f['category']}
                </span>
              </td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#555; font-size:12px;">
                {f['first_filing_date']}
              </td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee; font-size:12px; font-weight:600;">
                {first_effect_cell}
              </td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee;">
                <a href="{f['filing_url']}" style="color:#1a56db; text-decoration:none; font-size:12px;">
                  {f['accession']}
                </a>
              </td>
            </tr>"""

        body = f"""
        <table style="width:100%; border-collapse:collapse; font-size:14px;">
          <thead>
            <tr style="background:#f4f5f7;">
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">Company</th>
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">CIK</th>
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">Type</th>
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">First Filing</th>
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">1st EFFECT?</th>
              <th style="padding:10px 12px; text-align:left; font-weight:700; color:#333;">Accession #</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>"""

    count = len(filings)
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             background:#f9fafb; margin:0; padding:0;">
  <div style="max-width:860px; margin:32px auto; background:#fff;
              border-radius:8px; overflow:hidden;
              box-shadow:0 1px 4px rgba(0,0,0,0.1);">

    <div style="background:#1a3a6e; padding:24px 32px; color:#fff;">
      <h1 style="margin:0; font-size:20px; font-weight:700;">
        EDGAR EFFECT Filings
      </h1>
      <p style="margin:6px 0 0; opacity:0.85; font-size:14px;">{date_str}</p>
    </div>

    <div style="padding:24px 32px;">
      <p style="color:#555; margin:0 0 20px;">
        <strong>{count}</strong> EFFECT filing{"s" if count != 1 else ""} found on EDGAR.
        These represent registration statements that became effective - potential
        IPOs, follow-on offerings, shelf registrations, SPACs, and mergers.
      </p>
      {body}
    </div>

    <div style="padding:16px 32px; background:#f4f5f7; font-size:12px; color:#888;">
      Source: <a href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=EFFECT"
                 style="color:#1a56db;">SEC EDGAR</a> &mdash;
      Generated automatically by the EDGAR EFFECT scraper.
    </div>
  </div>
</body>
</html>"""


def send_email(subject: str, html_body: str) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ["SMTP_USERNAME"]
    smtp_pass = os.environ["SMTP_PASSWORD"]
    from_addr = os.environ["EMAIL_FROM"]
    to_addrs = [a.strip() for a in os.environ["EMAIL_TO"].split(",")]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg.attach(MIMEText(html_body, "html"))

    log.info("Sending email to %s via %s:%d", to_addrs, smtp_host, smtp_port)
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, to_addrs, msg.as_string())
    log.info("Email sent successfully.")


def main() -> None:
    date_override = os.environ.get("EDGAR_DATE")
    if date_override:
        from datetime import datetime
        filing_date = datetime.strptime(date_override, "%Y-%m-%d").date()
    else:
        filing_date = get_previous_business_day()

    log.info("Target filing date: %s", filing_date)

    raw_hits = fetch_effect_filings(filing_date)

    if not raw_hits:
        log.info("No EFFECT filings found for %s.", filing_date)

    filings = parse_filings(raw_hits)
    log.info("Enriching %d filings with offering category...", len(filings))
    for f in filings:
        if f["cik"]:
            info = get_company_info(f["cik"])
            f["category"] = info["category"]
            f["first_filing_date"] = info["first_filing_date"]
            f["is_first_effect"] = info["is_first_effect"]
        else:
            f["category"] = "Unknown"
            f["first_filing_date"] = ""
            f["is_first_effect"] = False
        log.info(
            "  %s -> %s (first filing: %s, first EFFECT: %s)",
            f["company"], f["category"], f["first_filing_date"], f["is_first_effect"],
        )

    category_order = {
        "IPO": 0,
        "IPO (Foreign)": 1,
        "REIT IPO": 2,
        "SEO / Shelf": 3,
        "SEO / Shelf (Foreign)": 4,
        "Merger / SPAC": 5,
        "Merger (Foreign)": 6,
        "Other": 7,
        "Unknown": 8,
    }
    filings.sort(key=lambda f: (category_order.get(f["category"], 99), f["company"]))

    date_label = filing_date.strftime("%Y-%m-%d")
    subject = f"EDGAR EFFECT Filings - {date_label} ({len(filings)} filing{'s' if len(filings) != 1 else ''})"
    html = build_html_email(filings, filing_date)
    send_email(subject, html)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
