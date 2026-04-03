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

EDGAR_FULL_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{}.json"
EDGAR_FILING_BASE = "https://www.sec.gov/edgar/browse/?CIK={}"

# SEC requires a descriptive User-Agent
HEADERS = {
    "User-Agent": os.environ.get(
        "EDGAR_USER_AGENT", "IPOTracker/1.0 research@example.com"
    ),
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

# Registration form types → offering category
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


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def get_previous_business_day() -> date:
    """Return yesterday, rolling back over weekends."""
    today = date.today()
    if today.weekday() == 0:      # Monday → Friday
        return today - timedelta(days=3)
    return today - timedelta(days=1)


def quarter_for_date(d: date) -> int:
    return (d.month - 1) // 3 + 1


def fetch_effect_filings(filing_date: date) -> list[dict]:
    """
    Return all EFFECT filings from the EDGAR full-index for the given date.
    Uses the quarterly company.idx file rather than the EFTS search API,
    because EFFECT forms contain almost no text and are not indexed by EFTS.
    """
    date_str = filing_date.strftime("%Y-%m-%d")
    year     = filing_date.year
    quarter  = quarter_for_date(filing_date)

    idx_url = EDGAR_FULL_INDEX_URL.format(year=year, quarter=quarter)
    log.info("Fetching full-index for %s (Q%d %d): %s", date_str, quarter, year, idx_url)

    # Retry up to 3 times on 5xx errors
    for attempt in range(3):
        resp = requests.get(idx_url, headers=HEADERS, timeout=60)
        if resp.status_code < 500:
            break
        wait = 2 ** attempt
        log.warning("EDGAR returned %d, retrying in %ds...", resp.status_code, wait)
        time.sleep(wait)

    if resp.status_code == 404:
        log.warning("Full-index not yet available for Q%d %d — skipping.", quarter, year)
        return []

    if resp.status_code >= 500:
        log.error("EDGAR full-index unavailable (HTTP %d) after retries — skipping.", resp.status_code)
        return []

    resp.raise_for_status()

    # Parse the fixed-width company.idx file.
    # Column positions are read dynamically from the header line so the
    # parser is robust to any variation in the SEC's file layout.
    lines = resp.text.splitlines()

    # Locate the header and separator lines to determine column positions
    col_company = col_form = col_cik = col_date = col_file = None
    data_start_idx = 0

    for i, line in enumerate(lines):
        if "Form Type" in line and "CIK" in line and "Date Filed" in line:
            col_company = line.index("Company Name") if "Company Name" in line else 0
            col_form    = line.index("Form Type")
            col_cik     = line.index("CIK")
            col_date    = line.index("Date Filed")
            col_file    = line.index("Filename")
        if line.startswith("---") and col_form is not None:
            data_start_idx = i + 1
            break

    if col_form is None:
        log.error("Could not locate header in company.idx — file format may have changed.")
        return []

    log.info("Column positions — Company:%d Form:%d CIK:%d Date:%d File:%d",
             col_company, col_form, col_cik, col_date, col_file)

    # Log a sample data line so we can verify column alignment
    sample = next((l for l in lines[data_start_idx:] if len(l) > col_file), None)
    if sample:
        log.info("Sample line  : %r", sample[:120])
        log.info("Parsed fields: company=%r form=%r cik=%r date=%r",
                 sample[col_company:col_form].strip(),
                 sample[col_form:col_cik].strip(),
                 sample[col_cik:col_date].strip(),
                 sample[col_date:col_file].strip())

    filings = []
    total_effect = 0

    for line in lines[data_start_idx:]:
        if len(line) <= col_file:
            continue

        form_type = line[col_form:col_cik].strip()
        file_date  = line[col_date:col_file].strip()

        if form_type == "EFFECT":
            total_effect += 1

        if form_type != "EFFECT" or file_date != date_str:
            continue

        company_name = line[col_company:col_form].strip()
        cik_padded   = line[col_cik:col_date].strip().zfill(10)
        cik          = cik_padded.lstrip("0") or "0"
        filename     = line[col_file:].strip()

        # Derive accession number from filename
        # e.g. edgar/data/1234567/0001234567-25-000001.txt
        accession      = ""
        accession_path = ""
        if filename:
            basename = filename.rsplit("/", 1)[-1]
            accession = basename.replace(".txt", "").replace(".htm", "")
            accession_path = accession.replace("-", "")

        filings.append({
            "company":           company_name,
            "cik":               cik,
            "sic":               "",   # filled in during enrichment
            "accession":         accession,
            "file_date":         file_date,
            "first_filing_date": "",   # filled in during enrichment
            "filing_url": (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/"
                if cik and accession_path
                else "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=EFFECT"
            ),
            "edgar_url": EDGAR_FILING_BASE.format(cik_padded) if cik else "",
        })

    log.info("Found %d EFFECT filings for %s (out of %d total EFFECT rows in Q%d %d index)",
             len(filings), date_str, total_effect, quarter, year)
    return filings


def get_company_info(cik: str) -> dict:
    """
    Fetch company submissions and return:
      - sic: SIC code string
      - first_filing_date: earliest filing date on record (YYYY-MM-DD string)
      - effect_count: total number of EFFECT filings in recent history
      - category: offering type based on registration form history
    """
    url = EDGAR_SUBMISSIONS_URL.format(cik.zfill(10))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        sic = str(data.get("sic", "") or "")

        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])

        # Earliest filing date — dates are most-recent-first, so take the last one
        first_filing_date = dates[-1] if dates else ""

        # Count how many EFFECT filings this company has had
        effect_count = forms.count("EFFECT")

        # Determine category from first non-EFFECT registration form
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
            "sic":               sic,
            "first_filing_date": first_filing_date,
            "effect_count":      effect_count,
            "category":          category,
        }
    except requests.HTTPError as exc:
        log.warning("HTTP error fetching submissions for CIK %s: %s", cik, exc)
        return {"sic": "", "first_filing_date": "", "effect_count": 0, "category": "Unknown"}
    except Exception as exc:
        log.warning("Error fetching submissions for CIK %s: %s", cik, exc)
        return {"sic": "", "first_filing_date": "", "effect_count": 0, "category": "Unknown"}
    finally:
        time.sleep(0.15)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

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


TABLE_HEADER = (
    "<table style='width:100%;border-collapse:collapse;font-size:14px;'>"
    "<thead><tr style='background:#f4f5f7;'>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Company</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>CIK</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>SIC</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>First Filing</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>1st EFFECT</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Accession #</th>"
    "</tr></thead><tbody>"
)
SPAC_TABLE_HEADER = (
    "<table style='width:100%;border-collapse:collapse;font-size:14px;'>"
    "<thead><tr style='background:#f4f5f7;'>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Company</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>CIK</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>SIC</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>First Filing</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>1st EFFECT</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Accession #</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>PCAOB</th>"
    "</tr></thead><tbody>"
)
TABLE_FOOTER = "</tbody></table>"


def build_row(f: dict, show_pcaob: bool = False) -> str:
    sic_style = "font-weight:700;color:#b8860b;" if f["sic"] == "6770" else "color:#555;"
    sic_label = f["sic"] + (" &#9733;" if f["sic"] == "6770" else "")
    is_first = f.get("effect_count", 0) <= 1
    first_label = "&#10003; Yes" if is_first else "No"
    first_style = "color:#1a7f3c;font-weight:700;" if is_first else "color:#999;"

    pcaob_cell = ""
    if show_pcaob and f.get("cik"):
        pcaob_url = "https://pcaobus.org/resources/auditorsearch/issuers/?issuercik=" + f["cik"]
        pcaob_cell = (
            "<td style='padding:8px 12px;border-bottom:1px solid #eee;'>"
            "<a href='" + pcaob_url + "' style='color:#1a56db;text-decoration:none;font-size:12px;'>View &#8599;</a>"
            "</td>"
        )

    return (
        "<tr>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;'>"
        "<a href='" + f["edgar_url"] + "' style='color:#1a56db;text-decoration:none;font-weight:600;'>"
        + f["company"] +
        "</a></td>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;color:#555;'>" + f["cik"] + "</td>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;" + sic_style + "'>" + sic_label + "</td>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;color:#555;font-size:12px;'>" + f["first_filing_date"] + "</td>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;font-size:12px;" + first_style + "'>" + first_label + "</td>"
        "<td style='padding:8px 12px;border-bottom:1px solid #eee;'>"
        "<a href='" + f["filing_url"] + "' style='color:#1a56db;text-decoration:none;font-size:12px;'>"
        + f["accession"] +
        "</a></td>"
        + pcaob_cell +
        "</tr>"
    )


def build_html_email(filings: list[dict], filing_date: date) -> str:
    date_str = filing_date.strftime("%A, %B %-d, %Y")
    count = len(filings)

    spacs = [f for f in filings if f["sic"] == "6770"]
    others = [f for f in filings if f["sic"] != "6770"]

    def make_table(rows: list[dict], title: str, subtitle: str, show_pcaob: bool = False) -> str:
        if not rows:
            return ""
        header = SPAC_TABLE_HEADER if show_pcaob else TABLE_HEADER
        row_html = "".join(build_row(r, show_pcaob=show_pcaob) for r in rows)
        return (
            "<h2 style='margin:24px 0 4px;font-size:16px;color:#1a3a6e;'>" + title + "</h2>"
            "<p style='margin:0 0 12px;font-size:13px;color:#777;'>" + subtitle + "</p>"
            + header + row_html + TABLE_FOOTER
        )

    if not filings:
        body = "<p style='color:#555;'>No EFFECT filings were found on EDGAR for this date.</p>"
    else:
        body = (
            make_table(
                spacs,
                "Special Purpose Acquisition Company (SPAC) Filings",
                "Blank check companies — likely SPAC IPOs or follow-on SPAC registrations.",
                show_pcaob=True,
            )
            + ("<div style='margin:24px 0;border-top:2px solid #eee;'></div>" if spacs and others else "")
            + make_table(
                others,
                "All Other EFFECT Filings",
                "IPOs, follow-on offerings, shelf registrations, mergers, and other registration types.",
            )
        )

    filing_count_label = str(count) + " EFFECT filing" + ("s" if count != 1 else "")

    return (
        "<!DOCTYPE html><html><head><meta charset='UTF-8'></head>"
        "<body style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#f9fafb;margin:0;padding:0;'>"
        "<div style='max-width:900px;margin:32px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.1);'>"
        "<div style='background:#1a3a6e;padding:24px 32px;color:#fff;'>"
        "<h1 style='margin:0;font-size:20px;font-weight:700;'>Filed Effective Forms on EDGAR</h1>"
        "<p style='margin:6px 0 0;opacity:0.85;font-size:14px;'>" + date_str + "</p>"
        "</div>"
        "<div style='padding:24px 32px;'>"
        "<p style='color:#555;margin:0 0 8px;'><strong>" + filing_count_label + "</strong> found on EDGAR.</p>"
        + body +
        "</div>"
        "<div style='padding:16px 32px;background:#f4f5f7;font-size:12px;color:#888;'>"
        "Source: <a href='https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&amp;type=EFFECT' style='color:#1a56db;'>SEC EDGAR</a> "
        "&mdash; Generated automatically by the EDGAR EFFECT scraper."
        "</div></div></body></html>"
    )


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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

CATEGORY_ORDER = {
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


def business_days_in_range(start: date, end: date) -> list[date]:
    """Return all weekdays (Mon–Fri) between start and end inclusive."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:   # 0=Mon … 4=Fri
            days.append(current)
        current += timedelta(days=1)
    return days


def process_one_day(filing_date: date) -> None:
    """Fetch, enrich, and email filings for a single date."""
    log.info("Processing %s", filing_date)

    filings = fetch_effect_filings(filing_date)
    if not filings:
        log.info("No EFFECT filings found for %s — skipping email.", filing_date)
        return

    log.info("Enriching %d filings...", len(filings))
    for f in filings:
        if f["cik"]:
            info = get_company_info(f["cik"])
            f["sic"]               = info["sic"]
            f["category"]          = info["category"]
            f["first_filing_date"] = info["first_filing_date"]
            f["effect_count"]      = info["effect_count"]
        else:
            f["sic"]               = ""
            f["category"]          = "Unknown"
            f["first_filing_date"] = ""
            f["effect_count"]      = 0
        log.info("  %s -> SIC:%s %s (first filing: %s, effect count: %d)",
                 f["company"], f["sic"], f["category"], f["first_filing_date"], f["effect_count"])

    filings.sort(key=lambda f: (CATEGORY_ORDER.get(f.get("category", "Unknown"), 99), f["company"]))

    date_label = filing_date.strftime("%Y-%m-%d")
    subject = f"Filed Effective Forms on EDGAR — {date_label} ({len(filings)} filing{'s' if len(filings) != 1 else ''})"
    html = build_html_email(filings, filing_date)
    send_email(subject, html)
    log.info("Email sent for %s.", filing_date)


def main() -> None:
    from datetime import datetime

    start_str = os.environ.get("EDGAR_START_DATE", "").strip()
    end_str   = os.environ.get("EDGAR_END_DATE",   "").strip()
    date_str  = os.environ.get("EDGAR_DATE",        "").strip()

    if start_str and end_str:
        # ── Range mode: backfill a date range ──────────────────────────────
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
        if start > end:
            log.error("EDGAR_START_DATE must be before or equal to EDGAR_END_DATE.")
            return
        days = business_days_in_range(start, end)
        log.info("Range mode: %d business day(s) from %s to %s", len(days), start, end)
        for i, day in enumerate(days):
            process_one_day(day)
            if i < len(days) - 1:
                time.sleep(2)   # brief pause between days
    elif date_str:
        # ── Single-date override ───────────────────────────────────────────
        filing_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        process_one_day(filing_date)
    else:
        # ── Default: previous business day ────────────────────────────────
        process_one_day(get_previous_business_day())


if __name__ == "__main__":
    main()
