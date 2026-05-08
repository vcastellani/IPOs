####################################################################################################
##### TITLE: SPAC EDGAR SCRAPER                                                                #####
##### AUTHOR: VINCENT CASTELLANI                                                               #####
##### NOTE: THIS FETCHES ALL SUSPECTED SPAC EFFECTIVE FILINGS FOR A MONTH, EMAILS A SUMMARY,   #####  
#####       AND UPDATES THE DATABASE.                                                          #####
####################################################################################################

# Import Standard Modules 

import os
import sys
import time
import smtplib
import logging
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests



# Configure Log

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s",
    stream = sys.stdout,
)
log = logging.getLogger(__name__)



# Define Constants - EDGAR URLS, SUPABASE, EDGAR API, and SEC Forms

EDGAR_SEARCH_URL         = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS_URL    = "https://data.sec.gov/submissions/CIK{}.json"
EDGAR_FILING_BASE        = "https://www.sec.gov/edgar/browse/?CIK={}"

SUPABASE_URL             = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY", "")

HEADERS = {
    "User-Agent": os.environ.get(
        "EDGAR_USER_AGENT", "IPOTracker/1.0 research@example.com"
    ),
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

FORM_CATEGORIES = {
    "S-1": "IPO", "S-1/A": "IPO",
    "F-1": "IPO (Foreign)", "F-1/A": "IPO (Foreign)",
    "S-11": "REIT IPO", "S-11/A": "REIT IPO",
    "S-3": "SEO / Shelf", "S-3/A": "SEO / Shelf", "S-3ASR": "SEO / Shelf",
    "S-4": "Merger / SPAC", "S-4/A": "Merger / SPAC",
    "F-3": "SEO / Shelf (Foreign)", "F-3/A": "SEO / Shelf (Foreign)",
    "F-4": "Merger (Foreign)", "F-4/A": "Merger (Foreign)",
}


# Define Function - _default_month, which calculates the first and last date of the month two calendar months ago.

def _default_month() -> tuple[date, date]:
    today         = date.today()
    first_this    = today.replace(day=1)
    last_prev     = first_this - timedelta(days=1)
    first_prev    = last_prev.replace(day=1)
    last_target   = first_prev - timedelta(days=1)
    first_target  = last_target.replace(day=1)
    return first_target, last_target



# Define Function - fetch_effect_filings, which queries EDGAR for all EFFECT filings

def fetch_effect_filings(start_date: date, end_date: date) -> list[dict]:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    log.info("Fetching EFFECT filings from %s to %s", start_str, end_str)

    filings: list[dict] = []
    from_idx = 0

    while True:
        for attempt in range(3):
            resp = requests.get(
                EDGAR_SEARCH_URL,
                params={
                    "forms": "EFFECT",
                    "dateRange": "custom",
                    "startdt": start_str,
                    "enddt": end_str,
                    "from": from_idx,
                    "size": 100,
                },
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code < 500:
                break
            wait = 2 ** attempt
            log.warning("EDGAR returned %d, retrying in %ds...", resp.status_code, wait)
            time.sleep(wait)

        if resp.status_code >= 500:
            log.error("EDGAR search unavailable (HTTP %d) after retries — skipping.", resp.status_code)
            return []

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
        data   = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])

        first_filing_date = dates[-1] if dates else ""
        effect_count      = forms.count("EFFECT")

        category = "Other"
        for form in forms:
            if form == "EFFECT":
                continue
            cat = FORM_CATEGORIES.get(form)
            if cat:
                category = cat
                break
            if form.startswith("S-") or form.startswith("F-"):
                category = "Other (" + form + ")"
                break

        return {
            "first_filing_date": first_filing_date,
            "effect_count": effect_count,
            "category": category,
        }
    except requests.HTTPError as exc:
        log.warning("HTTP error fetching submissions for CIK %s: %s", cik, exc)
        return {"first_filing_date": "", "effect_count": 0, "category": "Unknown"}
    except Exception as exc:
        log.warning("Error fetching submissions for CIK %s: %s", cik, exc)
        return {"first_filing_date": "", "effect_count": 0, "category": "Unknown"}
    finally:
        time.sleep(0.15)


def parse_filings(raw_hits: list[dict]) -> list[dict]:
    parsed = []
    for hit in raw_hits:
        src = hit.get("_source", {})

        display_names = src.get("display_names", [])
        company = display_names[0].split("(")[0].strip() if display_names else "N/A"

        ciks = src.get("ciks", [])
        cik  = ciks[0].lstrip("0") if ciks else ""

        accession      = src.get("adsh", "")
        accession_path = accession.replace("-", "")

        sics = src.get("sics", [])
        sic  = sics[0] if sics else ""

        parsed.append({
            "company":           company,
            "cik":               cik,
            "sic":               sic,
            "accession":         accession,
            "file_date":         src.get("file_date", ""),
            "first_filing_date": "",
            "effect_count":      0,
            "filing_url": (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/"
                if cik and accession_path
                else "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=EFFECT"
            ),
            "edgar_url": EDGAR_FILING_BASE.format(cik.zfill(10)) if cik else "",
        })
    return parsed


def push_pending_to_supabase(spacs: list[dict]) -> None:
    """Insert SPAC EFFECT filings into the pending_ipos Supabase table.
    Uses ignore-duplicates so re-runs are safe (cik is unique in the table).
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.warning("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — skipping pending queue push.")
        return

    rows = [
        {
            "company_name":    f["company"],
            "cik":             f["cik"],
            "effect_date":     f["file_date"],
            "is_first_effect": f.get("effect_count", 0) <= 1,
            "edgar_url":       f.get("edgar_url", ""),
        }
        for f in spacs
        if f.get("cik") and f.get("file_date")
    ]
    if not rows:
        return

    url = SUPABASE_URL.rstrip("/") + "/rest/v1/pending_ipos?on_conflict=cik"
    headers = {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=ignore-duplicates,return=minimal",
    }
    try:
        resp = requests.post(url, json=rows, headers=headers, timeout=15)
        if resp.status_code in (200, 201, 204):
            log.info("Pushed %d SPAC(s) to pending_ipos queue.", len(rows))
        else:
            log.warning("Failed to push to pending_ipos: HTTP %d — %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Error pushing to pending_ipos: %s", exc)


# ---------------------------------------------------------------------------
# Email  (Claude brand palette)
# ---------------------------------------------------------------------------
# Colors:
#   #D97757 — Claude signature warm orange (header, table header)
#   #1A1A1A — near-black text
#   #FFFFFF  — white (page background, primary rows)
#   #F5F0EB — warm beige (alternate rows, footer)
#   #C95B2C — darker orange for links

_FONT        = "system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
_ORANGE      = "#D97757"
_ORANGE_DARK = "#B85C38"   # left accent bar + border highlights
_TEXT        = "#1A1A1A"
_ROW_BG_EVEN = "#FFFFFF"
_ROW_BG_ODD  = "#EDE5DC"   # more pronounced beige

_TH = (
    f"padding:12px 16px;font-weight:700;color:#FFFFFF;background:{_ORANGE};"
    f"font-family:{_FONT};font-size:15px;letter-spacing:0.02em;"
    f"border-top:2px solid {_ORANGE_DARK};border-bottom:3px solid {_ORANGE_DARK};"
)

SPAC_TABLE_HEADER = (
    f"<table style='width:100%;border-collapse:collapse;font-size:15px;"
    f"font-family:{_FONT};color:{_TEXT};border:2px solid {_ORANGE};"
    f"box-shadow:0 4px 16px rgba(0,0,0,0.12);'>"
    "<thead><tr>"
    f"<th style='{_TH}text-align:left;white-space:nowrap;'>Company</th>"
    f"<th style='{_TH}text-align:right;'>CIK</th>"
    f"<th style='{_TH}text-align:center;white-space:nowrap;'>Filing Date</th>"
    f"<th style='{_TH}text-align:center;'>First Filing?</th>"
    f"<th style='{_TH}text-align:center;'>EDGAR</th>"
    "</tr></thead><tbody>"
)
TABLE_FOOTER = "</tbody></table>"

_BADGE_YES = (
    "display:inline-block;background:#2D6A4F;color:#FFFFFF;"
    "border-radius:12px;padding:2px 10px;font-size:12px;font-weight:600;white-space:nowrap;"
)
_BADGE_NO = (
    "display:inline-block;background:#6B7280;color:#FFFFFF;"
    "border-radius:12px;padding:2px 10px;font-size:12px;font-weight:600;white-space:nowrap;"
)


def build_row(f: dict, idx: int = 0) -> str:
    is_first = f.get("effect_count", 0) <= 1
    badge    = f"<span style='{_BADGE_YES}'>Yes</span>" if is_first else f"<span style='{_BADGE_NO}'>No</span>"
    bg       = _ROW_BG_EVEN if idx % 2 == 0 else _ROW_BG_ODD
    cell     = f"padding:12px 16px;background:{bg};border:none;color:{_TEXT};"

    return (
        f"<tr>"
        f"<td style='{cell}font-weight:600;white-space:nowrap;'>{f['company']}</td>"
        f"<td style='{cell}text-align:right;'>{f['cik']}</td>"
        f"<td style='{cell}text-align:center;white-space:nowrap;'>{f['file_date']}</td>"
        f"<td style='{cell}text-align:center;'>{badge}</td>"
        f"<td style='{cell}text-align:center;'>"
        f"<a href='{f['edgar_url']}' style='color:{_TEXT};text-decoration:underline;'>View &#8599;</a>"
        f"</td>"
        f"</tr>"
    )


def build_html_email(spacs: list[dict], start: date, end: date) -> str:
    month_label = start.strftime("%B %Y")
    date_range  = f"{start.strftime('%B %-d')} – {end.strftime('%B %-d, %Y')}"
    row_html    = "".join(build_row(f, i) for i, f in enumerate(spacs))

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:32px 16px 48px;
             background:linear-gradient(160deg,#FDF6F0 0%,#EDE0D4 100%);
             font-family:{_FONT};color:{_TEXT};">

<div style="max-width:760px;margin:0 auto;background:#FFFFFF;border-radius:10px;
            overflow:hidden;box-shadow:0 4px 16px rgba(0,0,0,0.10);">

  <!-- Header -->
  <div style="background:{_ORANGE};border-left:4px solid {_ORANGE_DARK};
              padding:24px 32px 20px;box-shadow:0 2px 6px rgba(0,0,0,0.12);">
    <h1 style="margin:0 0 6px;font-size:22px;font-weight:300;color:#FFFFFF;line-height:1.2;">
      SPAC IPO Filings &mdash; {month_label}
    </h1>
    <p style="margin:0;font-size:12px;color:#F5EDE0;">{date_range}</p>
  </div>

  <!-- Divider -->
  <div style="height:3px;background:linear-gradient(90deg,{_ORANGE_DARK},{_ORANGE},#F0C4A8,transparent);"></div>

  <!-- Body -->
  <div style="padding:24px 28px 32px;">
    {SPAC_TABLE_HEADER}{row_html}{TABLE_FOOTER}
  </div>

  <!-- Footer -->
  <div style="padding:14px 28px;background:#F5EDE0;border-top:1px solid #E0CFC0;">
    <p style="margin:0;font-size:12px;color:#4A3F38;text-align:center;">
      Source:&nbsp;<a href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&amp;type=EFFECT"
        style="color:{_TEXT};text-decoration:underline;">SEC EDGAR</a>
      &nbsp;&middot;&nbsp;<span style="font-style:italic;">Vincent Castellani, PhD</span>
    </p>
  </div>

</div>
</body></html>"""


def send_email(subject: str, html_body: str) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ["SMTP_USERNAME"]
    smtp_pass = os.environ["SMTP_PASSWORD"]
    from_addr = os.environ["EMAIL_FROM"]
    to_addrs  = [a.strip() for a in os.environ["EMAIL_TO"].split(",")]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addrs)
    msg.attach(MIMEText(html_body, "html"))

    log.info("Sending email to %s via %s:%d", to_addrs, smtp_host, smtp_port)
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, to_addrs, msg.as_string())
    log.info("Email sent successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_month(start: date, end: date) -> None:
    log.info("Processing %s to %s", start, end)

    raw_hits = fetch_effect_filings(start, end)
    if not raw_hits:
        log.info("No EFFECT filings found for %s to %s — skipping.", start, end)
        return

    filings = parse_filings(raw_hits)
    log.info("Enriching %d filings...", len(filings))
    for f in filings:
        if f["cik"]:
            info = get_company_info(f["cik"])
            f["first_filing_date"] = info["first_filing_date"]
            f["effect_count"]      = info["effect_count"]
        log.info("  %s (SIC %s, effect count: %d)", f["company"], f["sic"], f["effect_count"])

    spacs = [f for f in filings if f["sic"] == "6770"]
    spacs.sort(key=lambda f: f["file_date"])

    push_pending_to_supabase(spacs)

    if not spacs:
        log.info("No SPAC filings (SIC 6770) found for %s to %s — skipping email.", start, end)
        return

    month_label = start.strftime("%B %Y")
    subject     = f"💸 SPAC IPO Filings — {month_label}"
    html        = build_html_email(spacs, start, end)
    send_email(subject, html)
    log.info("Email sent: %d SPAC(s) for %s.", len(spacs), month_label)


def main() -> None:
    from datetime import datetime

    start_str = os.environ.get("EDGAR_START_DATE", "").strip()
    end_str   = os.environ.get("EDGAR_END_DATE",   "").strip()

    def _parse(s: str, var: str) -> date:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            log.error("%s=%r is not a valid YYYY-MM-DD date.", var, s)
            raise SystemExit(1)

    if start_str and end_str:
        start = _parse(start_str, "EDGAR_START_DATE")
        end   = _parse(end_str,   "EDGAR_END_DATE")
        if start > end:
            log.error("EDGAR_START_DATE must be before or equal to EDGAR_END_DATE.")
            raise SystemExit(1)
        log.info("Manual range: %s to %s", start, end)
    else:
        start, end = _default_month()
        log.info("Auto-computed range (2 months ago): %s to %s", start, end)

    process_month(start, end)


if __name__ == "__main__":
    main()
