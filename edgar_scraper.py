"""
edgar_scraper.py  –  Daily EDGAR EFFECT filing digest
Runs once per day (via cron or manually).  Sends an HTML email summary.
"""

from __future__ import annotations

import os
import smtplib
import ssl
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# ── Config ────────────────────────────────────────────────────────────────────

EDGAR_BASE   = "https://efts.sec.gov/LATEST/search-index?q=%22EFFECT%22&dateRange=custom"
EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index?q=%22EFFECT%22&dateRange=custom&startdt={start}&enddt={end}&forms=EFFECT"
FILING_URL   = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=EFFECT&dateb=&owner=include&count=10"

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 465))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
EMAIL_TO  = os.environ.get("EMAIL_TO", "")

# Known non-SPAC filers to suppress (add CIKs as strings)
SUPPRESS_CIKS: set[str] = set()

# Keywords that strongly suggest a SPAC
SPAC_KEYWORDS = [
    "acquisition", "blank check", "special purpose",
    "business combination", "sponsor", "founder shares",
    "trust account", "warrants", "redemption",
]

# ── HTML primitives ───────────────────────────────────────────────────────────

STYLE = """
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f0f2f5; margin: 0; padding: 24px; color: #1a1a2e; }
  .wrapper { max-width: 900px; margin: 0 auto; }
  .header  { background: linear-gradient(135deg,#1a56db,#0e9f6e);
             border-radius:12px; padding:28px 32px; margin-bottom:24px; color:#fff; }
  .header h1 { margin:0 0 4px; font-size:22px; font-weight:700; }
  .header p  { margin:0; opacity:.85; font-size:14px; }
  .section   { background:#fff; border-radius:10px; margin-bottom:20px;
               box-shadow:0 1px 4px rgba(0,0,0,.08); overflow:hidden; }
  .sec-title { padding:14px 20px; font-size:15px; font-weight:700;
               border-bottom:1px solid #f0f2f5; display:flex;
               align-items:center; gap:8px; }
  .badge { display:inline-block; padding:2px 10px; border-radius:20px;
           font-size:12px; font-weight:600; }
  .footer { text-align:center; color:#888; font-size:12px; padding:16px 0; }
</style>
"""

TABLE_HEADER = (
    "<table style='width:100%;border-collapse:collapse;font-size:14px;'>"
    "<thead><tr style='background:#f4f5f7;'>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Company</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>CIK</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Filed</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Filing</th>"
    "</tr></thead><tbody>"
)

SPAC_TABLE_HEADER = (
    "<table style='width:100%;border-collapse:collapse;font-size:14px;'>"
    "<thead><tr style='background:#f4f5f7;'>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Company</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>CIK</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Filed</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>Filing</th>"
    "<th style='padding:10px 12px;text-align:left;font-weight:700;color:#333;'>PCAOB</th>"
    "</tr></thead><tbody>"
)

# ── EDGAR fetch ───────────────────────────────────────────────────────────────

def fetch_filings(start: date, end: date) -> list[dict]:
    url = EDGAR_SEARCH.format(start=start.isoformat(), end=end.isoformat())
    headers = {"User-Agent": "edgar-digest contact@example.com"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])

    results = []
    for h in hits:
        src = h.get("_source", {})
        entity_name = src.get("entity_name") or src.get("display_names", [""])[0]
        cik_raw = src.get("file_num") or src.get("period_of_report") or ""
        cik = src.get("entity_id") or ""
        filed = src.get("file_date", "")
        accession = h.get("_id", "").replace(":", "-")
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}-index.htm" if cik and accession else ""
        form = src.get("form_type", "EFFECT")

        results.append({
            "name":    entity_name,
            "cik":     cik,
            "filed":   filed,
            "url":     filing_url,
            "form":    form,
            "desc":    src.get("file_description", ""),
        })
    return results


def classify(filings: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (spacs, suspected, others)."""
    spacs, suspected, others = [], [], []
    for f in filings:
        if f["cik"] in SUPPRESS_CIKS:
            continue
        name_lower = f["name"].lower()
        desc_lower = f["desc"].lower()
        combined   = name_lower + " " + desc_lower

        spac_score = sum(1 for kw in SPAC_KEYWORDS if kw in combined)

        if spac_score >= 3 or "acquisition corp" in name_lower or "blank check" in combined:
            spacs.append(f)
        elif spac_score >= 1 or "acquisition" in name_lower:
            suspected.append(f)
        else:
            others.append(f)
    return spacs, suspected, others


# ── HTML builders ─────────────────────────────────────────────────────────────

def build_row(f: dict, show_pcaob: bool = False) -> str:
    name_cell = (
        f"<td style='padding:8px 12px;border-bottom:1px solid #eee;font-weight:600;'>{f['name']}</td>"
    )
    cik_cell = (
        f"<td style='padding:8px 12px;border-bottom:1px solid #eee;'>"
        f"<a href='{FILING_URL.format(cik=f['cik'])}' "
        f"style='color:#1a56db;text-decoration:none;font-size:12px;'>{f['cik']}</a>"
        f"</td>"
    )
    filed_cell = (
        f"<td style='padding:8px 12px;border-bottom:1px solid #eee;color:#555;'>{f['filed']}</td>"
    )
    filing_cell = (
        f"<td style='padding:8px 12px;border-bottom:1px solid #eee;'>"
        f"<a href='{f['url']}' style='color:#1a56db;text-decoration:none;'>{f['form']}</a>"
        f"</td>"
    )
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
        + name_cell
        + cik_cell
        + filed_cell
        + filing_cell
        + pcaob_cell
        + "</tr>"
    )


def make_table(rows: list[dict], title: str, subtitle: str,
               color: str = "#1a56db", show_pcaob: bool = False) -> str:
    if not rows:
        return ""
    header = SPAC_TABLE_HEADER if show_pcaob else TABLE_HEADER
    row_html = "".join(build_row(r, show_pcaob=show_pcaob) for r in rows)
    count = len(rows)
    badge_color = {"#e53e3e": "#fff2f2", "#1a56db": "#eff6ff", "#0e9f6e": "#f0fdf4"}.get(color, "#f4f5f7")
    return f"""
<div class='section'>
  <div class='sec-title' style='background:{badge_color};'>
    <span style='font-size:18px;'>{'🔴' if color=='#e53e3e' else '🔵' if color=='#1a56db' else '🟢'}</span>
    {title}
    <span class='badge' style='background:{color};color:#fff;margin-left:auto;'>{count}</span>
  </div>
  <p style='margin:0;padding:8px 20px 12px;font-size:13px;color:#666;'>{subtitle}</p>
  {header}
  {row_html}
  </tbody></table>
</div>
"""


def build_email(spacs: list[dict], suspected: list[dict], others: list[dict],
                report_date: date) -> str:
    total = len(spacs) + len(suspected) + len(others)
    spac_section      = make_table(spacs,     "Confirmed SPACs",   "Classified as SPACs based on filing keywords.", color="#e53e3e", show_pcaob=False)
    suspected_section = make_table(suspected, "Suspected SPACs",   "Possible SPACs — review manually.", color="#1a56db", show_pcaob=True)
    other_section     = make_table(others,    "Other Registrants", "Likely non-SPAC filers.", color="#0e9f6e")

    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'>{STYLE}</head><body>
<div class='wrapper'>
  <div class='header'>
    <h1>📋 EDGAR EFFECT Filing Digest</h1>
    <p>{report_date.strftime('%A, %B %d, %Y')} &nbsp;·&nbsp; {total} filing(s) found</p>
  </div>
  {spac_section}
  {suspected_section}
  {other_section}
  <div class='footer'>Generated by edgar_scraper.py · Data from SEC EDGAR</div>
</div>
</body></html>"""


# ── Email send ────────────────────────────────────────────────────────────────

def send_email(subject: str, html: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    today     = date.today()
    yesterday = today - timedelta(days=1)

    print(f"Fetching EDGAR EFFECT filings for {yesterday} …")
    filings = fetch_filings(yesterday, yesterday)
    print(f"  {len(filings)} filing(s) fetched.")

    spacs, suspected, others = classify(filings)
    print(f"  SPACs={len(spacs)}  Suspected={len(suspected)}  Others={len(others)}")

    if not filings:
        print("Nothing to report — skipping email.")
        return

    html    = build_email(spacs, suspected, others, yesterday)
    subject = f"EDGAR EFFECT Digest — {yesterday.isoformat()} ({len(filings)} filing(s))"

    if SMTP_USER and SMTP_PASS and EMAIL_TO:
        send_email(subject, html)
        print("Email sent.")
    else:
        out = f"edgar_digest_{yesterday.isoformat()}.html"
        with open(out, "w") as fh:
            fh.write(html)
        print(f"Email creds not set — saved to {out}")


if __name__ == "__main__":
    main()
