import streamlit as st
from supabase import create_client, Client
import pandas as pd
from datetime import date, datetime, timedelta
import requests
import zipfile
import io
import re
import json
import difflib
import anthropic

st.set_page_config(
    page_title="SPAC Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────

EXCHANGES      = ["", "NYSE", "NASDAQ", "AMEX", "Other"]
SECURITY_TYPES = [
    "Shares",
    "Units - Shares and Warrants",
    "Units - Shares and Rights",
    "Units - Shares, Warrants, and Rights",
]
PP_SECURITY_TYPES = [
    "",
    "Shares",
    "Warrants",
    "Units - Shares and Warrants",
    "Units - Shares and Rights",
    "Units - Shares, Warrants, and Rights",
]
FILING_TYPES = ["S-1", "S-1/A", "8-K (IPO)", "8-K (Combination)", "424B4", "10-K", "Other"]

# ── Supabase connections ───────────────────────────────────────────────────────

@st.cache_resource
def anon_client() -> Client:
    return create_client(
        st.secrets["supabase"]["url"],
        st.secrets["supabase"]["anon_key"],
    )

def service_client() -> Client:
    return create_client(
        st.secrets["supabase"]["url"],
        st.secrets["supabase"]["service_role_key"],
    )

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_ipos() -> pd.DataFrame:
    resp = (
        anon_client()
        .table("ipos")
        .select("*")
        .order("ipo_date", desc=True)
        .execute()
    )
    if not resp.data:
        return pd.DataFrame()
    return pd.DataFrame(resp.data)

@st.cache_data(ttl=60)
def load_watchlist() -> pd.DataFrame:
    resp = (
        anon_client()
        .table("watchlist")
        .select("*")
        .order("created_at", desc=True)
        .execute()
    )
    if not resp.data:
        return pd.DataFrame()
    return pd.DataFrame(resp.data)

@st.cache_data(ttl=300)
def load_spac_audit_partners() -> pd.DataFrame:
    """Load pcaob_partners rows only for partner IDs referenced in the ipos table."""
    ipos_df = load_ipos()
    partner_ids = ipos_df["audit_partner_id"].dropna().unique().tolist()
    if not partner_ids:
        return pd.DataFrame()
    resp = (
        anon_client()
        .table("pcaob_partners")
        .select("*")
        .in_("engagement_partner_id", partner_ids)
        .execute()
    )
    if not resp.data:
        return pd.DataFrame()
    return pd.DataFrame(resp.data)
    
@st.cache_data(ttl=300)
def load_known_auditors() -> list[str]:
    df = load_ipos()
    if df.empty or "auditor" not in df.columns:
        return []
    return sorted(df["auditor"].dropna().unique().tolist())

@st.cache_data(ttl=300)
def load_known_underwriters() -> list[str]:
    df = load_ipos()
    if df.empty or "underwriters_list" not in df.columns:
        return []
    all_uws = []
    for val in df["underwriters_list"].dropna():
        if isinstance(val, list):
            all_uws.extend(val)
    return sorted(set(u for u in all_uws if u and u.strip()))

@st.cache_data(ttl=86400, show_spinner=False)
def load_pcaob_form_ap() -> pd.DataFrame:
    r = requests.get(
        "https://pcaobus.org/assets/PCAOBFiles/FirmFilings.zip",
        headers={"User-Agent": "SPACTracker/1.0 research@example.com"},
        timeout=120,
    )
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        csv_name = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, dtype=str, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    return df

def lookup_audit_partner(cik: str, audit_report_date: str) -> tuple[str | None, str | None]:
    """Returns (partner_id, debug_message)."""
    from datetime import datetime as _dt
    try:
        df = load_pcaob_form_ap()

        # Filter to standard issuer audits only
        type_col = next((c for c in df.columns if "audit report type" in c.lower()), None)
        if type_col:
            df = df[df[type_col].str.strip() == "Issuer, other than Employee Benefit Plan or Investment Company"]

        # Locate the CIK column case-insensitively
        cik_col = next((c for c in df.columns if c.lower().replace(" ", "") == "issuercik"), None)
        if cik_col is None:
            return None, f"CIK column not found. Columns: {list(df.columns)[:8]}"

        cik_padded = f"{int(cik):010d}"
        cik_plain  = str(int(cik))
        subset = df[df[cik_col].str.strip() == cik_padded]
        if subset.empty:
            subset = df[df[cik_col].str.strip().str.lstrip("0") == cik_plain]
        if subset.empty:
            return None, f"No Form AP rows found for CIK {cik_padded}"

        # Locate the audit report date column case-insensitively
        date_col = next((c for c in df.columns if "audit report date" in c.lower()), None)
        if date_col is None:
            return None, f"Audit Report Date column not found. Columns: {list(df.columns)[:8]}"

        # Locate the partner ID column
        pid_col = next((c for c in df.columns if "engagement partner id" in c.lower()), None)
        if pid_col is None:
            return None, "Engagement Partner ID column not found"

        target = None
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                target = _dt.strptime(audit_report_date.strip(), fmt).date()
                break
            except ValueError:
                continue
        if target is None:
            return None, f"Could not parse audit_report_date: {audit_report_date!r}"

        def _parse_date(s):
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
                try:
                    return _dt.strptime(str(s).strip(), fmt).date()
                except ValueError:
                    continue
            return None

        subset = subset.copy()
        subset["_date"] = subset[date_col].apply(_parse_date)
        match = subset[subset["_date"] == target]
        if match.empty:
            fallback = subset[subset["_date"].notna()]
            if fallback.empty:
                sample_dates = subset[date_col].dropna().head(3).tolist()
                return None, f"Found {len(subset)} rows for CIK but none had a parseable date. Raw date samples: {sample_dates}"
            match = fallback.iloc[(fallback["_date"].apply(lambda d: abs((d - target).days))).argsort()[:1]]

        pid = match.iloc[0].get(pid_col)
        if pd.notna(pid) and str(pid).strip():
            return str(pid).strip(), f"Found partner ID {pid} (date matched)"
        return None, "Row found but Engagement Partner ID was blank"
    except Exception as e:
        return None, f"Error: {e}"

@st.cache_resource
def anthropic_client():
    return anthropic.Anthropic(api_key=st.secrets.get("anthropic_api_key", ""))

def extract_from_424b4(url: str) -> dict:
    resp = requests.get(
        url,
        headers={"User-Agent": "SPACTracker/1.0 research@example.com"},
        timeout=30,
    )
    resp.raise_for_status()

    text = re.sub(r"<[^>]+>", " ", resp.text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    auditor_snippets = []
    # Grab focused windows around every "auditor since" phrase
    for m in re.finditer(r"We have served as the Company.{0,10}s auditor since", text, re.IGNORECASE):
        start = max(0, m.start() - 500)
        auditor_snippets.append(text[start:min(len(text), m.end() + 1500)])
    # Use the LAST audit report header (actual report, not table-of-contents entry)
    report_matches = list(re.finditer(r'REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM', text, re.IGNORECASE))
    if report_matches:
        start = max(0, report_matches[-1].start() - 200)
        auditor_snippets.append(text[start:min(len(text), start + 8000)])
    # Fall back to broader patterns if nothing found
    if not auditor_snippets:
        for pat in [r'EXPERTS', r'audited by', r'independent registered public accounting firm']:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                start = max(0, m.start() - 300)
                auditor_snippets.append(text[start:start + 5000])
                break
    auditor_section = "\n\n---\n\n".join(auditor_snippets)
    tail = text[-8000:]
    excerpt = text[:15000] + "\n\n[...]\n\n" + auditor_section + "\n\n[end of doc]\n\n" + tail

    prompt = (
        "Extract these fields from the SPAC 424B4 prospectus. Return ONLY a raw JSON object with no explanation:\n\n"
        "{\n"
        '  "company_name": "full legal company name",\n'
        '  "securities_offered": 12500000,\n'
        '  "securities_type": "Units - Shares and Warrants",\n'
        '  "auditor": "Audit firm name",\n'
        '  "auditor_since": 2021,\n'
        '  "audit_report_date": "February 15, 2024",\n'
        '  "overallotment_option": 1875000,\n'
        '  "underwriters": ["Lead Underwriter", "Co-Underwriter"],\n'
        '  "warrant_count": 0.5,\n'
        '  "warrant_strike_price": 11.50,\n'
        '  "rights_count": 0.1\n'
        "}\n\n"
        "Rules:\n"
        '- securities_type must be exactly one of: "Shares", "Units - Shares and Warrants", "Units - Shares and Rights", "Units - Shares, Warrants, and Rights"\n'
        "- securities_offered is the integer share/unit count (not a dollar amount)\n"
        "- warrant_count is warrants per unit (e.g. 0.5), null if not applicable\n"
        "- rights_count: find the phrase 'one right to receive [fraction] of one share' and convert the fraction directly to a decimal - 'one-fifth' = 0.2, 'one-half' = 0.5, 'one-tenth' = 0.1, 'one' (whole share) = 1.0. Do NOT derive this by dividing - read the fraction as stated. null if no rights.\n"
        "- warrant_strike_price is the exercise price in dollars, null if not applicable\n"
        '- auditor: find the "/s/ Firm Name" signature line near the end of the "REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM" section; the firm name repeats on the next line and may be followed by a website URL - ignore the URL, use only the firm name exactly as written after "/s/" (e.g. "MaloneBailey, LLP", "Marcum llp", "WithumSmith+Brown, PC")\n'
        "- auditor_since: find the exact phrase 'We have served as the Company\\'s auditor since YYYY' and extract YYYY as an integer; ignore every other year in the document including the report date and city date lines; null if not found\n"
        '- audit_report_date: the date formatted as "Month DD, YYYY" that appears on the line immediately before or after the "/s/ Firm Name" signature in the audit report — this is the date the auditors signed, NOT the prospectus date or IPO date; null if not found\n'
        "- overallotment_option: integer share/unit count the underwriters have the option to purchase - null if not found\n"
        "- underwriters: lead underwriter first, null if not found\n\n"
        "Filing text:\n"
        + excerpt
    )

    msg = anthropic_client().messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=[{
            "type": "text",
            "text": "You are a financial document parser for SEC filings. Output ONLY a raw JSON object. No explanation, no reasoning, no markdown, no prose — just the JSON object starting with { and ending with }.",
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[
            {"role": "user", "content": prompt},
        ],
    )

    raw = msg.content[0].text.strip()
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)?\}', raw, re.DOTALL)
    if json_match:
        raw = json_match.group(0)
    elif raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
    try:
        result = json.loads(raw)
        needs_fallback = (
            not result.get("auditor") or
            not result.get("auditor_since") or
            not result.get("audit_report_date")
        )
        if needs_fallback and auditor_section:
            aud_prompt = (
                "Extract auditor info from this SEC audit report section. Return ONLY a JSON object:\n"
                '{"auditor": "Firm Name", "auditor_since": 2024, "audit_report_date": "February 15, 2024"}\n\n'
                "- auditor: firm name from the '/s/ Firm Name' signature line\n"
                "- auditor_since: integer year from 'We have served as the Company\\'s auditor since YYYY'; ignore report dates and city dates\n"
                '- audit_report_date: "Month DD, YYYY" date on the line just before or after the /s/ signature; null if not found\n\n'
                "Audit report:\n" + auditor_section[-3000:]
            )
            aud_msg = anthropic_client().messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=[{"type": "text", "text": "Return only valid JSON, no markdown, no explanation."}],
                messages=[{"role": "user", "content": aud_prompt}],
            )
            aud_raw = aud_msg.content[0].text.strip()
            aud_match = re.search(r'\{.*?\}', aud_raw, re.DOTALL)
            if aud_match:
                try:
                    aud = json.loads(aud_match.group(0))
                    if not result.get("auditor"):
                        result["auditor"] = aud.get("auditor")
                    if not result.get("auditor_since"):
                        result["auditor_since"] = aud.get("auditor_since")
                    if not result.get("audit_report_date"):
                        result["audit_report_date"] = aud.get("audit_report_date")
                except json.JSONDecodeError:
                    pass
        return result
    except json.JSONDecodeError as e:
        raise ValueError(f"Claude returned non-JSON (first 300 chars): {raw[:300]}") from e


def find_edgar_urls(cik: str, effect_date: str) -> dict:
    from datetime import date as _date, timedelta
    cik_int = int(cik)
    resp = requests.get(
        f"https://data.sec.gov/submissions/CIK{cik_int:010d}.json",
        headers={"User-Agent": "SPACTracker/1.0 research@example.com"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    filings    = data.get("filings", {}).get("recent", {})
    forms      = filings.get("form", [])
    dates      = filings.get("filingDate", [])
    accessions = filings.get("accessionNumber", [])
    docs       = filings.get("primaryDocument", [])
    items_list = filings.get("items", [])

    effect_dt    = _date.fromisoformat(effect_date)
    window_start = effect_dt - timedelta(days=3)
    window_end   = effect_dt + timedelta(days=21)

    prospectus_url = None
    s1_url         = None
    s1_date        = None
    ipo_8k_url     = None
    ipo_8k_date    = None
    tenk_filings   = []  # list of (filed_dt, url) for all 10-Ks after effect_dt

    for i, form in enumerate(forms):
        filed_dt = _date.fromisoformat(dates[i])
        accession = accessions[i].replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}/{docs[i]}"
        if form in ("424B4", "424B3") and window_start <= filed_dt <= window_end and prospectus_url is None:
            prospectus_url = base
        if form == "S-1" and filed_dt < effect_dt:
            if s1_date is None or filed_dt > s1_date:
                s1_url  = base
                s1_date = filed_dt
        if form == "8-K" and filed_dt >= effect_dt:
            raw_items = items_list[i] if i < len(items_list) else ""
            item_parts = [p.strip() for p in str(raw_items).split(",")]
            if "1.01" in item_parts and "3.02" in item_parts:
                if ipo_8k_date is None or filed_dt < ipo_8k_date:
                    ipo_8k_url  = base
                    ipo_8k_date = filed_dt
        if form == "10-K" and filed_dt > effect_dt:
            tenk_filings.append((filed_dt, base))

    # Sort 10-Ks ascending by date so 1st, 2nd, 3rd order is correct
    tenk_filings.sort(key=lambda x: x[0])
    tenk_urls = [url for _, url in tenk_filings]

    if prospectus_url is None:
        raise ValueError(f"No 424B4 or 424B3 found for CIK {cik} within 3 days before / 21 days after {effect_date}")
    return {"prospectus_url": prospectus_url, "s1_url": s1_url, "ipo_8k_url": ipo_8k_url, "tenk_urls": tenk_urls}

def extract_from_8k(url: str) -> dict:
    resp = requests.get(
        url,
        headers={"User-Agent": "SPACTracker/1.0 research@example.com"},
        timeout=30,
    )
    resp.raise_for_status()

    text = re.sub(r"<[^>]+>", " ", resp.text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    excerpt = text[:25000]

    prompt = (
        "Extract these fields from a SPAC IPO consummation 8-K filing (Items 1.01 and 3.02). Return ONLY a raw JSON object:\n\n"
        "{\n"
        '  "ipo_date": "2024-01-15",\n'
        '  "ticker": "ACME",\n'
        '  "ticker_units": "ACMEU",\n'
        '  "ticker_warrants": "ACMEW",\n'
        '  "ticker_rights": "ACMER",\n'
        '  "exchange": "NASDAQ",\n'
        '  "overallotment_exercised": 1875000,\n'
        '  "overallotment_exercised_date": "2024-01-15",\n'
        '  "pp_securities": 500000,\n'
        '  "pp_securities_type": "Warrants",\n'
        '  "pp_price": 1.00,\n'
        '  "pp_securities_2": 12500,\n'
        '  "pp_securities_type_2": "Units - Shares and Warrants",\n'
        '  "pp_price_2": 10.00\n'
        "}\n\n"
        "Rules:\n"
        '- ipo_date: date the IPO was consummated/closed in YYYY-MM-DD format; look for "consummated its Initial Public Offering" or "closing of the Initial Public Offering"; null if not found\n'
        '- ticker: the common stock ticker symbol ONLY (NOT the units ticker) — this is the symbol under which shares of common stock trade separately, typically without a suffix (e.g. "ACME"); null if not found or if only units are listed\n'
        '- ticker_units: the units ticker symbol (typically ends in "U", e.g. "ACMEU"); null if units are not listed or not issued\n'
        '- ticker_warrants: the warrant ticker symbol (typically ends in "W" or "WS", e.g. "ACMEW"); null if no warrants\n'
        '- ticker_rights: the rights ticker symbol (typically ends in "R", e.g. "ACMER"); null if no rights\n'
        '- exchange: must be exactly one of "NYSE", "NASDAQ", "AMEX"; null if not found\n'
        '- overallotment_exercised: integer count of securities the underwriters purchased under the over-allotment/greenshoe option; null if not mentioned\n'
        '- overallotment_exercised_date: YYYY-MM-DD date the over-allotment was exercised; null if not found\n'
        '- pp_securities: integer count of the first private placement security sold simultaneously with the IPO (Item 3.02); null if not found\n'
        '- pp_securities_type: type of first PP security, must be exactly one of: "Shares", "Warrants", "Units - Shares and Warrants", "Rights", "Units - Shares and Rights", "Units - Shares, Warrants, and Rights"; null if not found\n'
        '- pp_price: price per unit/warrant/share of the first PP as a float; null if not found\n'
        '- pp_securities_2: integer count of a second distinct private placement security if one exists; null if not found\n'
        '- pp_securities_type_2: type of second PP security (same options); null if not found\n'
        '- pp_price_2: price per unit of the second PP as a float; null if not found\n\n'
        "Filing text:\n" + excerpt
    )

    msg = anthropic_client().messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        system=[{
            "type": "text",
            "text": "You are a financial document parser for SEC filings. Output ONLY a raw JSON object. No explanation, no reasoning, no markdown, no prose — just the JSON object starting with { and ending with }.",
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    raw = msg.content[0].text.strip()
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)?\}', raw, re.DOTALL)
    if json_match:
        raw = json_match.group(0)
    elif raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}

def _extract_registered_securities(raw_html: str) -> str:
    """Extract the Section 12(b) registered securities block from a 10-K.
    Tries HTML table parsing first; falls back to stripped-text extraction.
    """
    # ── Attempt 1: parse <table> structure ────────────────────────────────
    lower_html = raw_html.lower()
    idx = lower_html.find("section 12(b)")
    if idx == -1:
        idx = lower_html.find("trading symbol")
    if idx != -1:
        table_start = raw_html.find("<table", idx)
        if table_start != -1:
            table_end = raw_html.find("</table>", table_start)
            if table_end != -1:
                table_html = raw_html[table_start: table_end + 8]
                rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE)
                lines = []
                for row in rows:
                    cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL | re.IGNORECASE)
                    cells = [re.sub(r"<[^>]+>", " ", c) for c in cells]
                    cells = [re.sub(r"\s+", " ", c).strip() for c in cells]
                    cells = [c for c in cells if c and c.strip("\xa0")]
                    if len(cells) >= 2:
                        lines.append(" | ".join(cells))
                if len(lines) >= 2:  # at least header + one data row
                    return "\n".join(lines)

    # ── Fallback: extract the 12(b) text block from stripped HTML ─────────
    stripped = re.sub(r"<[^>]+>", " ", raw_html)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    lower_s = stripped.lower()

    start = lower_s.find("section 12(b)")
    if start == -1:
        start = lower_s.find("trading symbol")
    if start == -1:
        return ""

    end = lower_s.find("section 12(g)", start + 10)
    block = stripped[start: end + 50 if end != -1 else start + 2500]
    return block[:2500]


def _parse_tickers_from_sec12b(sec12b_text: str) -> dict:
    """Regex-based ticker extraction from Section 12(b) block; bypasses Claude.
    Returns a dict with any of: ticker, ticker_units, ticker_warrants, ticker_rights, exchange.
    """
    if not sec12b_text:
        return {}

    result: dict = {}

    def _exch(raw: str) -> str:
        r = raw.lower()
        if "nasdaq" in r:
            return "NASDAQ"
        if "amex" in r or "american" in r:
            return "AMEX"
        if "nyse" in r or "new york" in r:
            return "NYSE"
        return ""

    def _kind(desc: str) -> str:
        d = desc.lower()
        if "unit" in d:
            return "units"
        if "warrant" in d:
            return "warrants"
        if "right" in d:
            return "rights"
        if any(k in d for k in ("common stock", "ordinary share", "class a", "class b", "share", "stock")):
            return "common"
        return ""

    if "|" in sec12b_text:
        for line in sec12b_text.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 2:
                continue
            desc, ticker = parts[0], parts[1]
            exch_raw = parts[2] if len(parts) > 2 else ""
            if not ticker or not re.match(r"^[A-Z]", ticker):
                continue
            if ticker.lower() in ("trading symbol", "trading symbol(s)", "symbol", "ticker"):
                continue
            if not result.get("exchange") and exch_raw:
                e = _exch(exch_raw)
                if e:
                    result["exchange"] = e
            k = _kind(desc)
            if k == "units":
                result.setdefault("ticker_units", ticker)
            elif k == "warrants":
                result.setdefault("ticker_warrants", ticker)
            elif k == "rights":
                result.setdefault("ticker_rights", ticker)
            elif k == "common":
                result.setdefault("ticker", ticker)
    else:
        _EXCH_RE = r"(?:NASDAQ(?:\s+(?:CAPITAL\s+MARKET|GLOBAL\s+(?:SELECT\s+)?MARKET))?|NYSE(?:\s+(?:AMERICAN|ARCA|MKT))?|AMEX)"
        _TICK_RE = r"([A-Z]{3,7}(?:\s[A-Z]{1,3})?)"
        for m in re.finditer(_TICK_RE + r"\s+" + _EXCH_RE, sec12b_text):
            ticker = m.group(1)
            exch_raw = sec12b_text[m.start(): m.end()]
            before = sec12b_text[max(0, m.start() - 150): m.start()]
            if not result.get("exchange"):
                e = _exch(exch_raw)
                if e:
                    result["exchange"] = e
            k = _kind(before)
            if k == "units":
                result.setdefault("ticker_units", ticker)
            elif k == "warrants":
                result.setdefault("ticker_warrants", ticker)
            elif k == "rights":
                result.setdefault("ticker_rights", ticker)
            elif k == "common":
                result.setdefault("ticker", ticker)

    return result


def _parse_tickers_from_xbrl(raw_html: str) -> dict:
    """Extract tickers from iXBRL dei:TradingSymbol elements via StatementClassOfStockAxis context.
    Returns a dict with any of: ticker, ticker_units, ticker_warrants, ticker_rights, exchange.
    Falls back to ticker suffix classification when no axis context is found.
    """
    result: dict = {}
    _ATTR_RE = re.compile(r'([\w:]+)\s*=\s*["\']([^"\']*)["\']')

    # ── Step 1: collect contextRef → ticker from dei:TradingSymbol elements ──
    tickers_by_ctx: dict[str, str] = {}
    for m in re.finditer(
        r'<ix:nonNumeric\b([^>]+)>([^<]*)</ix:nonNumeric>',
        raw_html, re.IGNORECASE | re.DOTALL
    ):
        attrs = {k.lower(): v for k, v in _ATTR_RE.findall(m.group(1))}
        if attrs.get("name", "").lower() != "dei:tradingsymbol":
            continue
        ticker = m.group(2).strip()
        ctx = attrs.get("contextref", "")
        if ticker and ctx:
            tickers_by_ctx[ctx] = ticker

    if not tickers_by_ctx:
        return {}

    # ── Step 2: classify each context by its StatementClassOfStockAxis member ──
    def _member_kind(raw: str) -> str:
        m = raw.lower().split(":")[-1]
        if "unit" in m:
            return "units"
        if "warrant" in m:
            return "warrants"
        if "right" in m:
            return "rights"
        if any(k in m for k in ("classa", "classb", "commonstock", "ordinaryshare",
                                  "commonshare", "classacommon", "classbcommon")):
            return "common"
        return "other"

    def _suffix_kind(ticker: str) -> str:
        t = ticker.upper().replace(" ", "")
        if t.endswith("U"):
            return "units"
        if t.endswith(("WS", "WT", "W")):
            return "warrants"
        if t.endswith("R") and len(t) > 4:
            return "rights"
        return "common"

    ctx_to_kind: dict[str, str] = {}
    for cm in re.finditer(
        r'<xbrli:context\b[^>]*\bid\s*=\s*["\']([^"\']+)["\'][^>]*>(.*?)</xbrli:context>',
        raw_html, re.IGNORECASE | re.DOTALL
    ):
        ctx_id, ctx_body = cm.group(1), cm.group(2)
        am = re.search(
            r'dimension\s*=\s*["\'][^"\']*StatementClassOfStockAxis["\'][^>]*>\s*([^\s<]+)',
            ctx_body, re.IGNORECASE
        )
        ctx_to_kind[ctx_id] = _member_kind(am.group(1)) if am else "other"

    # ── Step 3: assign tickers to result fields ──
    def _assign(kind: str, ticker: str) -> None:
        if kind == "units":
            result.setdefault("ticker_units", ticker)
        elif kind == "warrants":
            result.setdefault("ticker_warrants", ticker)
        elif kind == "rights":
            result.setdefault("ticker_rights", ticker)
        else:
            result.setdefault("ticker", ticker)

    for ctx_ref, ticker in tickers_by_ctx.items():
        kind = ctx_to_kind.get(ctx_ref, "other")
        if kind == "other":
            kind = _suffix_kind(ticker)
        _assign(kind, ticker)

    # ── Step 4: exchange from dei:SecurityExchangeName ──
    for em in re.finditer(
        r'<ix:nonNumeric\b([^>]+)>([^<]+)</ix:nonNumeric>',
        raw_html, re.IGNORECASE | re.DOTALL
    ):
        attrs = {k.lower(): v for k, v in _ATTR_RE.findall(em.group(1))}
        if attrs.get("name", "").lower() != "dei:securityexchangename":
            continue
        exch = em.group(2).strip().upper()
        if "NASDAQ" in exch:
            result["exchange"] = "NASDAQ"
        elif "AMEX" in exch or "AMERICAN" in exch:
            result["exchange"] = "AMEX"
        elif "NYSE" in exch:
            result["exchange"] = "NYSE"
        break

    return result


def extract_from_10k(url: str) -> dict:
    """Extract key IPO/PP verification fields from a 10-K primary document."""
    resp = requests.get(
        url,
        headers={"User-Agent": "SPACTracker/1.0 research@example.com"},
        timeout=60,
    )
    resp.raise_for_status()

    text = re.sub(r"<[^>]+>", " ", resp.text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    lower = text.lower()
    # Find ALL anchor positions and pick the earliest one that isn't in the cover/ToC
    # (positions < 2000 are excluded as they'd be cover page or table of contents)
    _anchors = [
        "initial public offering and private placement",
        "consummated our initial public offering",
        "consummated the initial public offering",
        "completed our initial public offering",
        "closed our initial public offering",
        "consummated its initial public offering",
    ]
    _anchor_hits = []
    for _anchor in _anchors:
        _i = lower.find(_anchor)
        if _i > 2000:
            _anchor_hits.append((_i, _anchor))
    if _anchor_hits:
        idx, _matched_anchor = min(_anchor_hits, key=lambda x: x[0])
    else:
        idx, _matched_anchor = -1, None
    excerpt = text[max(0, idx - 300): idx + 10000] if idx != -1 else text[:12000]

    # Parse the Section 12(b) table from raw HTML before stripping tags
    sec12b_text = _extract_registered_securities(resp.text)

    # Also grab Item 5 (Market Information) for ticker symbols
    _item5_anchors = [
        "market for registrant",
        "item 5. market",
        "item\xa05.",
    ]
    _item5_idx = -1
    for _a5 in _item5_anchors:
        _i5 = lower.find(_a5)
        if _i5 != -1:
            _item5_idx = _i5
            break
    ticker_excerpt = text[_item5_idx: _item5_idx + 3000] if _item5_idx != -1 else ""

    _debug_info = {"anchor": _matched_anchor, "idx": idx, "excerpt_start": excerpt[:300], "sec12b": sec12b_text[:400]}

    prompt = (
        "Extract these fields from the IPO section of a SPAC 10-K annual report. "
        "Return ONLY a raw JSON object:\n\n"
        "{\n"
        '  "ipo_date": "2024-07-03",\n'
        '  "offer_price": 10.00,\n'
        '  "securities_offered": 20000000,\n'
        '  "overallotment_exercised": 3000000,\n'
        '  "overallotment_exercised_date": "2024-07-03",\n'
        '  "pp_securities": 6000000,\n'
        '  "pp_securities_type": "Warrants",\n'
        '  "pp_price": 1.00,\n'
        '  "pp_securities_2": null,\n'
        '  "pp_securities_type_2": null,\n'
        '  "pp_price_2": null,\n'
        '  "ticker": "ACME",\n'
        '  "ticker_units": "ACMEU",\n'
        '  "ticker_warrants": "ACMEW",\n'
        '  "ticker_rights": null,\n'
        '  "exchange": "NASDAQ"\n'
        "}\n\n"
        "Rules:\n"
        '- ipo_date: date the IPO was consummated/closed in YYYY-MM-DD format\n'
        '- offer_price: price per unit/share in the IPO as a float (e.g., 10.00)\n'
        '- securities_offered: BASE IPO count only (EXCLUDING over-allotment). '
        'IMPORTANT: if text says "X Units, including Y Units for the over-allotment", base = X - Y. '
        'If text says "X Units" with no mention of inclusion, use X as base.\n'
        '- overallotment_exercised: integer count of over-allotment securities; null if OA was not exercised\n'
        '- overallotment_exercised_date: date OA units were sold in YYYY-MM-DD. '
        'If OA was exercised "simultaneously" or "concurrently" with the IPO closing, use the same date as ipo_date. '
        'If OA was exercised on a separate later date, use that date. Null if OA not exercised.\n'
        '- pp_securities: TOTAL count of all private placement securities of the FIRST type sold '
        '"simultaneously with the closing" of the IPO (or simultaneously with the OA closing). '
        'Combine any separate tranches of the same security type (initial + OA-related). '
        'IMPORTANT: do NOT include founder shares / initial shares / insider shares / promoter shares '
        'issued to initial shareholders before the IPO for nominal consideration (e.g. $25,000 for '
        'millions of shares, or ~$0.003–$0.02 per share). Those are founder shares, not the private placement.\n'
        '- pp_securities_type: classify the first PP security as exactly one of: '
        '"Shares", "Warrants", "Units - Shares and Warrants", "Units - Shares and Rights", '
        '"Units - Shares, Warrants, and Rights". '
        '"Private Placement Warrants" = "Warrants". "Private Units" = match to the appropriate Units type based on what each unit contains.\n'
        '- pp_price: price per security of the first PP as a float (typically $10.00 per unit or $1.00–$1.50 per warrant)\n'
        '- pp_securities_2: TOTAL count of a second DISTINCT PP security type if one exists; null if none\n'
        '- pp_securities_type_2: type of second PP security (same options); null if none\n'
        '- pp_price_2: price per security of the second PP; null if none\n'
        '- ticker: common stock / ordinary shares ticker (no suffix), e.g. "LEGT" or "ACME"; '
        'find it in the Registered Securities table labeled "Ordinary Shares", "Class A Common Stock", etc.\n'
        '- ticker_units: units ticker — may have a space before the suffix, e.g. "LEGT U" or "ACMEU"; '
        'labeled "Units" in the securities table\n'
        '- ticker_warrants: warrant ticker — may have a space before suffix, e.g. "LEGT WS", "ACMEW", "ACME WT"; '
        'labeled "Warrants" in the securities table; null if no warrants\n'
        '- ticker_rights: rights ticker — may have a space before "R", e.g. "ACMER" or "ACME R"; '
        'labeled "Rights" in the securities table; null if no rights\n'
        '- exchange: exactly one of "NYSE", "NASDAQ", "AMEX"; null if not found\n\n'
        + ("Registered Securities (Section 12(b) table — pipe-separated: description | ticker | exchange):\n" + sec12b_text + "\n\n" if sec12b_text else "")
        + "IPO section:\n" + excerpt
        + ("\n\nMarket Information section:\n" + ticker_excerpt if ticker_excerpt else "")
    )

    msg = anthropic_client().messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=900,
        system=[{
            "type": "text",
            "text": "You are a financial document parser for SEC filings. Output ONLY a raw JSON object. No explanation, no reasoning, no markdown, no prose — just the JSON object starting with { and ending with }.",
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    raw = msg.content[0].text.strip()
    _debug_info["claude_raw"] = raw[:600]
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)?\}', raw, re.DOTALL)
    if json_match:
        raw = json_match.group(0)
    elif raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
    try:
        result = json.loads(raw)

        # Override Claude's ticker fields: try iXBRL dei:TradingSymbol first,
        # fall back to Section 12(b) regex if the filing lacks XBRL tags.
        _xbrl_tickers = _parse_tickers_from_xbrl(resp.text)
        _fallback_tickers = _parse_tickers_from_sec12b(sec12b_text) if not _xbrl_tickers else {}
        _ticker_override = _xbrl_tickers or _fallback_tickers
        for _field in ("ticker", "ticker_units", "ticker_warrants", "ticker_rights", "exchange"):
            if _ticker_override.get(_field):
                result[_field] = _ticker_override[_field]
        _debug_info["xbrl_tickers"] = _xbrl_tickers
        _debug_info["fallback_tickers"] = _fallback_tickers

        # When OA was exercised simultaneously with IPO, default the date to ipo_date
        if (result.get("overallotment_exercised")
                and not result.get("overallotment_exercised_date")
                and result.get("ipo_date")):
            result["overallotment_exercised_date"] = result["ipo_date"]

        result["_debug"] = _debug_info
        return result
    except json.JSONDecodeError:
        return {"_debug": _debug_info}


def refresh():
    st.cache_data.clear()

def _idx(lst, val, default=0):
    try:
        return lst.index(val)
    except ValueError:
        return default

def fmt_int(val):
    if val is None:
        return None
    try:
        return f"{int(val):,}"
    except (ValueError, TypeError):
        return str(val)

def fmt_warrants(val):
    if val is None:
        return None
    try:
        f = float(val)
        if f == int(f):
            return f"{int(f):,}"
        return f"{f:,.4f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        return str(val)

def oa_status(option, exercised):
    """Compute overallotment status from total option and exercised amount."""
    if not option:
        return None
    if exercised is None:
        return "Pending"
    elif exercised == 0:
        return "Expired"
    elif exercised >= option:
        return "Fully Exercised"
    else:
        return f"Partially Exercised ({fmt_int(exercised)} of {fmt_int(option)})"

def fmt_underwriters(lst):
    if not lst:
        return None
    return ", ".join(lst)

def resolve_pick(pick, new, other_label="Other / New..."):
    """Return the typed-in value if 'Other' was picked, otherwise the dropdown value."""
    if pick == other_label:
        return new.strip() or None
    return pick or None

def _fuzzy_match(val, known_list, cutoff=0.75):
    if not val:
        return None
    val_lower = val.lower().strip()
    for k in known_list:
        if k.lower() == val_lower:
            return k
    for k in known_list:
        if k.lower() in val_lower or val_lower in k.lower():
            return k
    lower_known = [k.lower() for k in known_list]
    m = difflib.get_close_matches(val_lower, lower_known, n=1, cutoff=cutoff)
    if m:
        return known_list[lower_known.index(m[0])]
    return None

# ── Session state ─────────────────────────────────────────────────────────────

if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📈 SPAC Tracker")
    st.caption("Tracking EDGAR EFFECT filings")
    st.divider()

    if not st.session_state.is_admin:
        with st.expander("Admin Login"):
            pwd = st.text_input("Password", type="password", key="pwd_input")
            if st.button("Login"):
                if pwd == st.secrets.get("admin_password", ""):
                    st.session_state.is_admin = True
                    st.rerun()
                else:
                    st.error("Incorrect password")
    else:
        st.success("Logged in as admin")
        if st.button("Logout"):
            st.session_state.is_admin = False
            st.rerun()

    st.divider()

    st.subheader("Filters")
    filter_exchange = st.multiselect("Exchange", ["NYSE", "NASDAQ", "AMEX", "Other"])
    search          = st.text_input("Search company name")

# ── Main table ────────────────────────────────────────────────────────────────

st.header("Special Purpose Acquisition Company (SPAC) IPOs")

df = load_ipos()

if not df.empty:
    if filter_exchange:
        df = df[df["exchange"].isin(filter_exchange)]
    if search:
        df = df[df["company_name"].str.contains(search, case=False, na=False)]

    def _get_424b4_url(filings_val):
        if not filings_val:
            return None
        for f in filings_val:
            if isinstance(f, dict) and f.get("type") == "424B4":
                return f.get("url")
        return None

    df["prospectus_url"] = df["filings"].apply(_get_424b4_url)
    _oa = df["overallotment_exercised"].fillna(0) if "overallotment_exercised" in df.columns else 0
    df["size_m"] = (
        (df["securities_offered"].fillna(0) + _oa)
        * df["offer_price"].fillna(0)
        / 1_000_000
    ).round(1)

    display_cols = [c for c in ["company_name", "cik", "ipo_date", "size_m", "prospectus_url", "verified"] if c in df.columns]

    col_cfg = {
        "company_name":   st.column_config.TextColumn("Company"),
        "cik":            st.column_config.TextColumn("CIK"),
        "ipo_date":       st.column_config.DateColumn("IPO Date", format="YYYY-MM-DD"),
        "size_m":         st.column_config.NumberColumn("Size ($M)", format="$ %.1f"),
        "prospectus_url": st.column_config.LinkColumn("Prospectus", display_text="📄"),
        "verified":       st.column_config.CheckboxColumn("Verified", disabled=True),
    }
    st.dataframe(
        df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config=col_cfg,
    )
    st.caption(f"{len(df)} filing(s) shown")

    dl1, dl2 = st.columns([1, 1])
    with dl1:
        st.download_button(
            "Download filtered view (.csv)",
            data=df.to_csv(index=False),
            file_name="spac_tracker_filtered.csv",
            mime="text/csv",
        )
    with dl2:
        full_export = load_ipos()
        st.download_button(
            "Download full database (.csv)",
            data=full_export.to_csv(index=False),
            file_name="spac_tracker_full.csv",
            mime="text/csv",
        )
else:
    st.info("No filings yet. Log in as admin to add entries.")

# ── Analytics ─────────────────────────────────────────────────────────────────

df_all = load_ipos()
df_dated = df_all[df_all["ipo_date"].notna()].copy()
if not df_dated.empty:
    df_dated["ipo_date"] = pd.to_datetime(df_dated["ipo_date"])
    yearly = (
        df_dated.groupby(df_dated["ipo_date"].dt.year)
        .size()
        .sort_index()
    )
    yearly.index = yearly.index.astype(str)
    yearly.name  = "SPAC IPOs"

    st.markdown("**SPAC IPOs per Year**")
    st.bar_chart(yearly, y_label="# of IPOs", x_label="Year")
    st.caption(f"{len(df_dated)} IPO(s) total across {len(yearly)} year(s)")
    st.divider()

# ── Detail view ───────────────────────────────────────────────────────────────

if not df.empty:
    with st.expander("Detail View"):
        names  = sorted(df["company_name"].dropna().tolist())
        chosen = st.selectbox("Select a company", names, key="detail_select")
        if chosen:
            row = df[df["company_name"] == chosen].iloc[0]
            if row.get("verified"):
                st.success("✅ Verified")
            col_info, col_img = st.columns([3, 1])
            with col_info:
                oa_date_str = None
                if row.get("ipo_date"):
                    oa_date = pd.to_datetime(row["ipo_date"]).date() + timedelta(days=45)
                    oa_date_str = oa_date.strftime("%B %d, %Y")

                total_str = None
                if row.get("offer_price") and row.get("securities_offered"):
                    total = row["offer_price"] * row["securities_offered"] / 1_000_000
                    total_str = f"${total:,.1f}M"

                oa_stat = oa_status(row.get("overallotment_option"), row.get("overallotment_exercised"))

                fields = {
                    "CIK":                  row.get("cik"),
                    "EDGAR Homepage":       f"[SEC Filing Page]({row['edgar_url']})" if row.get("edgar_url") else None,
                    "Common Stock Ticker":  row.get("ticker"),
                    "Units Ticker":         row.get("ticker_units"),
                    "Warrant Ticker":       row.get("ticker_warrants"),
                    "Rights Ticker":        row.get("ticker_rights"),
                    "Exchange":             row.get("exchange"),
                    "Auditor":              row.get("auditor"),
                    "Auditor Since":        row.get("auditor_since"),
                    "Audit Report Date":    row.get("audit_report_date"),
                    "Audit Partner ID":     row.get("audit_partner_id"),
                    "Effective Date":       row.get("effective_date"),
                    "IPO Date":             row.get("ipo_date"),
                    "Price":                f"${row['offer_price']:,.2f}" if row.get("offer_price") else None,
                    "Securities Type":      row.get("securities_type"),
                    "Securities Offered":   fmt_int(row.get("securities_offered")),
                    "Total Offering":       total_str,
                    "Warrants":             f"{fmt_warrants(row.get('warrant_count'))} @ ${row['warrant_strike_price']:,.2f}" if row.get("warrant_count") else None,
                    "Rights":               fmt_warrants(row.get("rights_count")),
                    "Overallotment Option":        f"{fmt_int(row.get('overallotment_option'))} securities" if row.get("overallotment_option") else None,
                    "Overallotment Expiry":        oa_date_str,
                    "Overallotment Status":        oa_stat,
                    "Overallotment Exercise Date": row.get("overallotment_exercised_date"),
                    "PP Securities (1)":      fmt_int(row.get("pp_securities")),
                    "PP Securities Type (1)": row.get("pp_securities_type"),
                    "PP Price (1)":           f"${row['pp_price']:,.2f}" if row.get("pp_price") else None,
                    "PP Securities (2)":      fmt_int(row.get("pp_securities_2")),
                    "PP Securities Type (2)": row.get("pp_securities_type_2"),
                    "PP Price (2)":           f"${row['pp_price_2']:,.2f}" if row.get("pp_price_2") else None,
                    "Underwriters":         fmt_underwriters(row.get("underwriters_list")),
                    "Notes":                row.get("notes"),
                }
                for label, val in fields.items():
                    if val is not None and val != "":
                        st.markdown(f"**{label}:** {val}")

                filings = row.get("filings") or []
                if filings:
                    st.markdown("**Filings:**")
                    for f in filings:
                        label = f.get("desc") or f["url"]
                        st.markdown(f"- **{f['type']}**: [{label}]({f['url']})")

            with col_img:
                img = row.get("image_url")
                if img:
                    st.markdown(f"<img src='{img}' style='width:100%;border-radius:8px;'>", unsafe_allow_html=True)

# ── Admin panel ───────────────────────────────────────────────────────────────

if st.session_state.is_admin:
    st.divider()
    st.subheader("Admin Panel")
    tab_add, tab_edit, tab_verify = st.tabs(["Add New Entry", "Edit / Delete", "IPO Verification"])

    # ── Add ───────────────────────────────────────────────────────────────────
    with tab_add:
        # Outside-form selectors for instant reactivity
        if "prefill_sec_type_pending" in st.session_state:
            st.session_state["add_sec_type"] = st.session_state.pop("prefill_sec_type_pending")
        st.markdown("##### Securities Type")
        a_sec_type     = st.selectbox("Securities Type", SECURITY_TYPES, key="add_sec_type", label_visibility="collapsed")
        a_has_warrants = "Warrant" in a_sec_type
        a_has_rights   = "Right"   in a_sec_type

        st.markdown("**Pre-fill from EDGAR (CIK + EFFECT Date)**")
        pf_col1, pf_col2, pf_col3 = st.columns([2, 2, 1])
        with pf_col1:
            pf_cik = st.text_input(
                "CIK", key="pf_424b4_cik",
                label_visibility="collapsed",
                placeholder="CIK (e.g. 1926599)",
            )
        with pf_col2:
                        pf_date = st.date_input("EFFECT Date", value=None, key="pf_424b4_date", label_visibility="collapsed")
        with pf_col3:
            if st.button("Find & Extract", key="pf_extract", use_container_width=True):
                if pf_cik and pf_date:
                    with st.spinner("Looking up EDGAR..."):
                        try:
                            urls = find_edgar_urls(pf_cik, pf_date.isoformat())
                            pf_url = urls["prospectus_url"]
                            st.spinner("Reading prospectus...")
                            data = extract_from_424b4(pf_url)
                            data["prospectus_url"] = pf_url
                            data["s1_url"] = urls.get("s1_url")
                            data["ipo_8k_url"] = urls.get("ipo_8k_url")
                            data["tenk_urls"] = urls.get("tenk_urls", [])
                            if urls.get("ipo_8k_url"):
                                try:
                                    k8_data = extract_from_8k(urls["ipo_8k_url"])
                                    for k, v in k8_data.items():
                                        if v is not None and not data.get(k):
                                            data[k] = v
                                    if data.get("overallotment_exercised") and not data.get("overallotment_exercised_date"):
                                        data["overallotment_exercised_date"] = data.get("ipo_date")
                                except Exception:
                                    pass
                            data["cik"] = f"{int(pf_cik):010d}"
                            data["effective_date"] = pf_date.isoformat()
                            cik_int = int(pf_cik)
                            data["edgar_url"] = f"https://www.sec.gov/edgar/browse/?CIK={cik_int:010d}"
                            if data.get("audit_report_date") and not data.get("audit_partner_id"):
                                with st.spinner("Looking up PCAOB audit partner…"):
                                    _pid, _dbg = lookup_audit_partner(pf_cik, data["audit_report_date"])
                                    data["audit_partner_id"] = _pid
                            st.session_state.prefill_424b4 = data
                            if data.get("securities_type") in SECURITY_TYPES:
                                st.session_state["prefill_sec_type_pending"] = data["securities_type"]
                            st.success(f"Found 424B4 — review fields below and submit.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")
                else:
                    st.warning("Enter both CIK and filing date.")

        pf = st.session_state.get("prefill_424b4", {})
        pf_uws = pf.get("underwriters") or []

        _btn_cols = st.columns(2)
        with _btn_cols[0]:
            if pf.get("prospectus_url"):
                st.link_button("424B4 Prospectus 📄", pf["prospectus_url"], use_container_width=True)
        with _btn_cols[1]:
            if pf.get("ipo_8k_url"):
                st.link_button("IPO 8-K Filing 📄", pf["ipo_8k_url"], use_container_width=True)

        st.divider()

        with st.form("add_form", clear_on_submit=True):
            st.markdown("**Initial Filings**")
            fi1, fi2, fi3 = st.columns(3)
            with fi1:
                a_s1_url = st.text_input("S-1 URL", value=pf.get("s1_url") or "")
            with fi2:
                a_8k_url = st.text_input("8-K URL", value=pf.get("ipo_8k_url") or "")
            with fi3:
                a_prospectus_url = st.text_input("Prospectus (424B4) URL", value=pf.get("prospectus_url", ""))


            c1, c2, c3 = st.columns(3)

            with c1:
                st.markdown("**Company**")
                a_name = st.text_input("Company Name *", value=pf.get("company_name", ""))
                a_cik           = st.text_input("CIK", value=pf.get("cik", ""))
                a_edgar_url     = st.text_input("EDGAR Homepage URL", value=pf.get("edgar_url", ""))
                a_ticker          = st.text_input("Common Stock Ticker", value=pf.get("ticker") or "")
                a_ticker_units    = st.text_input("Units Ticker", value=pf.get("ticker_units") or "")
                a_ticker_warrants = st.text_input("Warrant Ticker", value=pf.get("ticker_warrants") or "")
                a_ticker_rights   = st.text_input("Rights Ticker", value=pf.get("ticker_rights") or "")
                a_exchange        = st.selectbox("Exchange", EXCHANGES, index=_idx(EXCHANGES, pf.get("exchange") or ""))
                _known_aud    = load_known_auditors()
                _aud_opts     = [""] + _known_aud + ["Other / New..."]
                pf_auditor_raw = pf.get("auditor") or ""
                _aud_matched   = _fuzzy_match(pf_auditor_raw, _known_aud)
                pf_auditor     = _aud_matched if _aud_matched else pf_auditor_raw
                _aud_idx       = _aud_opts.index(pf_auditor) if pf_auditor in _known_aud else (len(_aud_opts) - 1 if pf_auditor else 0)
                a_auditor_sel = st.selectbox("Auditor", _aud_opts, index=_aud_idx)
                a_auditor_new = st.text_input("New auditor name", value=pf_auditor if pf_auditor not in _known_aud else "", placeholder="Type if not listed above")
                a_auditor_since     = st.text_input("Auditor Since", value=str(pf.get("auditor_since", "")) if pf.get("auditor_since") else "")
                a_audit_report_date = st.text_input("Audit Report Date", value=pf.get("audit_report_date") or "")
                a_audit_partner_id  = st.text_input("Audit Partner ID", value=pf.get("audit_partner_id") or "")
                a_image             = st.text_input("Image URL")

            with c2:
                st.markdown("**Dates & Pricing**")
                a_effective = pf.get("effective_date")
                _pf_ipo = pd.to_datetime(pf["ipo_date"]).date() if pf.get("ipo_date") else None
                a_ipo       = st.date_input("IPO Date", value=_pf_ipo)

                st.markdown("**Underwriters**")
                _known_uws = load_known_underwriters()
                _uw_opts   = [""] + _known_uws + ["Other / New..."]
                def _pf_uw_idx(val):
                    if not val:
                        return 0
                    m = _fuzzy_match(val, _known_uws)
                    if m:
                        return _uw_opts.index(m)
                    return len(_uw_opts) - 1
                def _pf_uw_new(val):
                    if not val:
                        return ""
                    return "" if _fuzzy_match(val, _known_uws) else val
                uwc1, uwc2 = st.columns(2)
                with uwc1:
                    pf_uw0 = pf_uws[0] if len(pf_uws) > 0 else ""
                    a_uw_1_sel = st.selectbox("Underwriter 1 (Lead)", _uw_opts, index=_pf_uw_idx(pf_uw0))
                    a_uw_1_new = st.text_input("New name 1", value=_pf_uw_new(pf_uw0), placeholder="Type if not listed")
                    pf_uw2 = pf_uws[2] if len(pf_uws) > 2 else ""
                    a_uw_3_sel = st.selectbox("Underwriter 3", _uw_opts, index=_pf_uw_idx(pf_uw2))
                    a_uw_3_new = st.text_input("New name 3", value=_pf_uw_new(pf_uw2), placeholder="Type if not listed")
                    pf_uw4 = pf_uws[4] if len(pf_uws) > 4 else ""
                    a_uw_5_sel = st.selectbox("Underwriter 5", _uw_opts, index=_pf_uw_idx(pf_uw4))
                    a_uw_5_new = st.text_input("New name 5", value=_pf_uw_new(pf_uw4), placeholder="Type if not listed")
                with uwc2:
                    pf_uw1 = pf_uws[1] if len(pf_uws) > 1 else ""
                    a_uw_2_sel = st.selectbox("Underwriter 2", _uw_opts, index=_pf_uw_idx(pf_uw1))
                    a_uw_2_new = st.text_input("New name 2", value=_pf_uw_new(pf_uw1), placeholder="Type if not listed")
                    pf_uw3 = pf_uws[3] if len(pf_uws) > 3 else ""
                    a_uw_4_sel = st.selectbox("Underwriter 4", _uw_opts, index=_pf_uw_idx(pf_uw3))
                    a_uw_4_new = st.text_input("New name 4", value=_pf_uw_new(pf_uw3), placeholder="Type if not listed")
                    pf_uw5 = pf_uws[5] if len(pf_uws) > 5 else ""
                    a_uw_6_sel = st.selectbox("Underwriter 6", _uw_opts, index=_pf_uw_idx(pf_uw5))
                    a_uw_6_new = st.text_input("New name 6", value=_pf_uw_new(pf_uw5), placeholder="Type if not listed")
                a_uw_others = [
                    resolve_pick(a_uw_2_sel, a_uw_2_new),
                    resolve_pick(a_uw_3_sel, a_uw_3_new),
                    resolve_pick(a_uw_4_sel, a_uw_4_new),
                    resolve_pick(a_uw_5_sel, a_uw_5_new),
                    resolve_pick(a_uw_6_sel, a_uw_6_new),
                ]

                


            with c3:
                st.markdown("**Securities**")
                a_securities = st.number_input("Securities Offered", min_value=0, step=100_000,
                                value=int(pf["securities_offered"]) if pf.get("securities_offered") else None)

                if a_has_warrants:
                    a_warrant_count = st.number_input("Number of Warrants", min_value=0.0, step=0.5,
                                   value=float(pf["warrant_count"]) if pf.get("warrant_count") else None)
                    a_warrant_strike = st.number_input("Warrant Strike Price ($)", min_value=0.0, step=0.01,
                                    value=float(pf["warrant_strike_price"]) if pf.get("warrant_strike_price") else None)
                else:
                    a_warrant_count  = None
                    a_warrant_strike = None

                if a_has_rights:
                    a_rights_count = st.number_input("Number of Rights", min_value=0.0, step=0.5, value=float(pf["rights_count"]) if pf.get("rights_count") else None)

                else:
                    a_rights_count = None

                st.markdown("**Overallotment**")
                a_oa_option         = st.number_input("Total Option (securities)", min_value=0, step=100_000, value=int(pf["overallotment_option"]) if pf.get("overallotment_option") else None)
                a_oa_exercised      = st.number_input("Exercised (securities)", min_value=0, step=100_000, value=int(pf["overallotment_exercised"]) if pf.get("overallotment_exercised") else None)
                _pf_oa_date = pd.to_datetime(pf["overallotment_exercised_date"]).date() if pf.get("overallotment_exercised_date") else None
                a_oa_exercised_date = st.date_input("Exercise Date", value=_pf_oa_date, key="add_oa_ex_date")

            st.markdown("**Private Placement**")
            pp1, pp2, pp3 = st.columns(3)
            with pp1:
                a_pp_securities = st.number_input("PP Securities (1)", min_value=0, step=100_000, value=int(pf["pp_securities"]) if pf.get("pp_securities") else None)
            with pp2:
                a_pp_sec_type = st.selectbox("PP Securities Type (1)", PP_SECURITY_TYPES, index=_idx(PP_SECURITY_TYPES, pf.get("pp_securities_type") or ""))
            with pp3:
                a_pp_price = st.number_input("PP Price (1) ($)", min_value=0.0, step=0.01, value=float(pf["pp_price"]) if pf.get("pp_price") else None)
            pp4, pp5, pp6 = st.columns(3)
            with pp4:
                a_pp_securities_2 = st.number_input("PP Securities (2)", min_value=0, step=100_000, value=int(pf["pp_securities_2"]) if pf.get("pp_securities_2") else None)
            with pp5:
                a_pp_sec_type_2 = st.selectbox("PP Securities Type (2)", PP_SECURITY_TYPES, index=_idx(PP_SECURITY_TYPES, pf.get("pp_securities_type_2") or ""))
            with pp6:
                a_pp_price_2 = st.number_input("PP Price (2) ($)", min_value=0.0, step=0.01, value=float(pf["pp_price_2"]) if pf.get("pp_price_2") else None)

            st.markdown("**Other**")
            a_notes = st.text_area("Notes")

            if st.form_submit_button("Add Entry", type="primary"):
                if not a_name:
                    st.error("Company Name is required.")
                else:
                    a_uw_1  = resolve_pick(a_uw_1_sel, a_uw_1_new)
                    uw_list = [u for u in [a_uw_1] + a_uw_others if u and u.strip()]

                    initial_filings = []
                    if a_s1_url:
                        initial_filings.append({"type": "S-1", "url": a_s1_url})
                    if a_8k_url:
                        initial_filings.append({"type": "8-K (IPO)", "url": a_8k_url})
                    if a_prospectus_url:
                        initial_filings.append({"type": "424B4", "url": a_prospectus_url})
                    _ordinals = ["1st", "2nd", "3rd", "4th", "5th"]
                    for _n, _url in enumerate(pf.get("tenk_urls") or []):
                        _desc = _ordinals[_n] if _n < len(_ordinals) else f"{_n+1}th"
                        initial_filings.append({"type": "10-K", "url": _url, "desc": _desc})

                    new_row = {
                        "company_name":           a_name,
                        "cik":                    a_cik or None,
                        "edgar_url":              a_edgar_url or None,
                        "ticker":                 a_ticker or None,
                        "ticker_units":           a_ticker_units or None,
                        "ticker_warrants":        a_ticker_warrants or None,
                        "ticker_rights":          a_ticker_rights or None,
                        "exchange":               a_exchange or None,
                        "auditor":                resolve_pick(a_auditor_sel, a_auditor_new) or None,
                        "auditor_since":          a_auditor_since or None,
                        "audit_report_date":      a_audit_report_date or None,
                        "audit_partner_id":       a_audit_partner_id or None,
                        "effective_date":         a_effective if a_effective else None,
                        "ipo_date":               a_ipo.isoformat() if a_ipo else None,
                        "offer_price":            10.00,
                        "securities_type":        a_sec_type,
                        "securities_offered":     int(a_securities) if a_securities else None,
                        "warrant_count":          float(a_warrant_count) if a_warrant_count else None,
                        "warrant_strike_price":   a_warrant_strike,
                        "rights_count":           float(a_rights_count) if a_rights_count else None,
                        "overallotment_option":   int(a_oa_option) if a_oa_option else None,
                        "overallotment_exercised":     int(a_oa_exercised) if a_oa_exercised is not None else None,
                        "overallotment_exercised_date":a_oa_exercised_date.isoformat() if a_oa_exercised_date else None,
                        "pp_securities":          int(a_pp_securities) if a_pp_securities else None,
                        "pp_securities_type":     a_pp_sec_type or None,
                        "pp_price":               a_pp_price,
                        "pp_securities_2":        int(a_pp_securities_2) if a_pp_securities_2 else None,
                        "pp_securities_type_2":   a_pp_sec_type_2 or None,
                        "pp_price_2":             a_pp_price_2,
                        "underwriters_list":      uw_list,
                        "notes":                  a_notes or None,
                        "image_url":              a_image or None,
                        "filings":                initial_filings,
                    }
                    service_client().table("ipos").insert(new_row).execute()
                    st.success(f"Added {a_name}!")
                    refresh()
                    st.session_state.pop("prefill_424b4", None)
                    st.rerun()

    # ── Edit / Delete ─────────────────────────────────────────────────────────
    with tab_edit:
        full_df = load_ipos()
        if full_df.empty:
            st.info("No entries to edit yet.")
        else:
            options = {
                f"{r['company_name']}  (ID {r['id']})": r["id"]
                for _, r in full_df.sort_values("company_name").iterrows()
        }
            sel_label = st.selectbox("Select entry", list(options.keys()), key="edit_select")
            sel_id    = options[sel_label]
            r         = full_df[full_df["id"] == sel_id].iloc[0]

            esel1, esel2 = st.columns(2)
            with esel1:
                st.markdown("##### Securities Type")
                e_sec_default  = r.get("securities_type") or SECURITY_TYPES[0]
                e_sec_type_key = f"edit_sec_type_{sel_id}"
                e_sec_type     = st.selectbox(
                    "Securities Type", SECURITY_TYPES,
                    index=_idx(SECURITY_TYPES, e_sec_default),
                    key=e_sec_type_key, label_visibility="collapsed",
                )
                e_has_warrants = "Warrant" in e_sec_type
                e_has_rights   = "Right"   in e_sec_type
            with esel2:
                st.markdown("##### Underwriters")
                existing_uws = r.get("underwriters_list") or []
                e_uw_default = "Multiple" if len(existing_uws) > 1 else "Solo"
                e_uw_mode    = st.radio("Underwriter count", ["Solo", "Multiple"],
                                        index=0 if e_uw_default == "Solo" else 1,
                                        horizontal=True, key=f"edit_uw_mode_{sel_id}")

            col_form, col_del = st.columns([4, 1])

            with col_form:
                with st.form("edit_form"):
                    ec1, ec2, ec3 = st.columns(3)

                    with ec1:
                        st.markdown("**Company**")
                        e_name          = st.text_input("Company Name", value=r.get("company_name", ""))
                        e_cik           = st.text_input("CIK", value=r.get("cik") or "")
                        e_edgar_url     = st.text_input("EDGAR Homepage URL", value=r.get("edgar_url") or "")
                        e_ticker          = st.text_input("Common Stock Ticker", value=r.get("ticker") or "")
                        e_ticker_units    = st.text_input("Units Ticker", value=r.get("ticker_units") or "")
                        e_ticker_warrants = st.text_input("Warrant Ticker", value=r.get("ticker_warrants") or "")
                        e_ticker_rights   = st.text_input("Rights Ticker", value=r.get("ticker_rights") or "")
                        e_exchange        = st.selectbox("Exchange", EXCHANGES, index=_idx(EXCHANGES, r.get("exchange") or ""))
                        _known_aud_e   = load_known_auditors()
                        _existing_aud  = r.get("auditor") or ""
                        _aud_opts_e    = [""] + _known_aud_e + ["Other / New..."]
                        _aud_idx       = _aud_opts_e.index(_existing_aud) if _existing_aud in _known_aud_e else (len(_aud_opts_e) - 1 if _existing_aud else 0)
                        e_auditor_sel  = st.selectbox("Auditor", _aud_opts_e, index=_aud_idx)
                        e_auditor_new  = st.text_input("New auditor name", value=_existing_aud if _existing_aud not in _known_aud_e else "", placeholder="Type if not listed above")
                        e_auditor_since     = st.text_input("Auditor Since", value=r.get("auditor_since") or "")
                        e_audit_report_date = st.text_input("Audit Report Date", value=r.get("audit_report_date") or "")
                        e_audit_partner_id  = st.text_input("Audit Partner ID", value=r.get("audit_partner_id") or "")
                        e_image            = st.text_input("Image URL", value=r.get("image_url") or "")

                    with ec2:
                        st.markdown("**Dates & Pricing**")
                        e_effective = st.date_input("Effective Date", value=pd.to_datetime(r["effective_date"]).date() if pd.notna(r.get("effective_date")) else None)
                        e_ipo       = st.date_input("IPO Date",       value=pd.to_datetime(r["ipo_date"]).date()       if pd.notna(r.get("ipo_date"))       else None)
                        e_offer     = st.number_input("Price ($)", value=float(r["offer_price"]) if pd.notna(r.get("offer_price")) else 0.0, step=0.01)

                        st.markdown("**Underwriters**")
                        _known_uws_e = load_known_underwriters()
                        _uw_opts_e   = [""] + _known_uws_e + ["Other / New..."]
                        
                        def _uw_sel_idx(val):
                            if val in _known_uws_e:
                                return _uw_opts_e.index(val)
                            return len(_uw_opts_e) - 1 if val else 0
                        
                        def _uw_new_val(val):
                            return val if val and val not in _known_uws_e else ""
                        
                        uw0 = existing_uws[0] if len(existing_uws) > 0 else ""
                        if e_uw_mode == "Solo":
                            e_uw_1_sel  = st.selectbox("Underwriter", _uw_opts_e, index=_uw_sel_idx(uw0))
                            e_uw_1_new  = st.text_input("New underwriter name", value=_uw_new_val(uw0), placeholder="Type if not listed above")
                            e_uw_others = []
                        else:
                            uwc1, uwc2 = st.columns(2)
                            with uwc1:
                                v1 = uw0
                                v3 = existing_uws[2] if len(existing_uws) > 2 else ""
                                v5 = existing_uws[4] if len(existing_uws) > 4 else ""
                                e_uw_1_sel = st.selectbox("Underwriter 1 (Lead)", _uw_opts_e, index=_uw_sel_idx(v1))
                                e_uw_1_new = st.text_input("New name 1", value=_uw_new_val(v1), placeholder="Type if not listed")
                                e_uw_3_sel = st.selectbox("Underwriter 3", _uw_opts_e, index=_uw_sel_idx(v3))
                                e_uw_3_new = st.text_input("New name 3", value=_uw_new_val(v3), placeholder="Type if not listed")
                                e_uw_5_sel = st.selectbox("Underwriter 5", _uw_opts_e, index=_uw_sel_idx(v5))
                                e_uw_5_new = st.text_input("New name 5", value=_uw_new_val(v5), placeholder="Type if not listed")
                            with uwc2:
                                v2 = existing_uws[1] if len(existing_uws) > 1 else ""
                                v4 = existing_uws[3] if len(existing_uws) > 3 else ""
                                v6 = existing_uws[5] if len(existing_uws) > 5 else ""
                                e_uw_2_sel = st.selectbox("Underwriter 2", _uw_opts_e, index=_uw_sel_idx(v2))
                                e_uw_2_new = st.text_input("New name 2", value=_uw_new_val(v2), placeholder="Type if not listed")
                                e_uw_4_sel = st.selectbox("Underwriter 4", _uw_opts_e, index=_uw_sel_idx(v4))
                                e_uw_4_new = st.text_input("New name 4", value=_uw_new_val(v4), placeholder="Type if not listed")
                                e_uw_6_sel = st.selectbox("Underwriter 6", _uw_opts_e, index=_uw_sel_idx(v6))
                                e_uw_6_new = st.text_input("New name 6", value=_uw_new_val(v6), placeholder="Type if not listed")
                            e_uw_others = [
                                resolve_pick(e_uw_2_sel, e_uw_2_new),
                                resolve_pick(e_uw_3_sel, e_uw_3_new),
                                resolve_pick(e_uw_4_sel, e_uw_4_new),
                                resolve_pick(e_uw_5_sel, e_uw_5_new),
                                resolve_pick(e_uw_6_sel, e_uw_6_new),
                            ]


                    with ec3:
                        st.markdown("**Securities**")
                        e_securities = st.number_input("Securities Offered", value=int(r["securities_offered"]) if pd.notna(r.get("securities_offered")) else 0, step=100_000)

                        if e_has_warrants:
                            e_warrant_count  = st.number_input("Number of Warrants", value=float(r["warrant_count"]) if pd.notna(r.get("warrant_count")) else 0.0, step=0.5)
                            e_warrant_strike = st.number_input("Warrant Strike Price ($)", value=float(r["warrant_strike_price"]) if pd.notna(r.get("warrant_strike_price")) else 0.0, step=0.01)
                        else:
                            e_warrant_count  = None
                            e_warrant_strike = None

                        if e_has_rights:
                            e_rights_count = st.number_input("Number of Rights", value=float(r["rights_count"]) if pd.notna(r.get("rights_count")) else 0.0, step=0.5)
                        else:
                            e_rights_count = None

                        st.markdown("**Overallotment**")
                        e_oa_option         = st.number_input("Total Option (securities)", value=int(r["overallotment_option"]) if pd.notna(r.get("overallotment_option")) else 0, step=100_000)
                        e_oa_exercised      = st.number_input("Exercised (securities)", value=int(r["overallotment_exercised"]) if pd.notna(r.get("overallotment_exercised")) else 0, step=100_000)
                        e_oa_exercised_date = st.date_input("Exercise Date", value=pd.to_datetime(r["overallotment_exercised_date"]).date() if pd.notna(r.get("overallotment_exercised_date")) else None)

                    st.markdown("**Private Placement**")
                    epp1, epp2, epp3 = st.columns(3)
                    with epp1:
                        e_pp_securities = st.number_input("PP Securities (1)", value=int(r["pp_securities"]) if pd.notna(r.get("pp_securities")) else 0, step=100_000)
                    with epp2:
                        e_pp_sec_type = st.selectbox("PP Securities Type (1)", PP_SECURITY_TYPES, index=_idx(PP_SECURITY_TYPES, r.get("pp_securities_type") or ""))
                    with epp3:
                        e_pp_price = st.number_input("PP Price (1) ($)", value=float(r["pp_price"]) if pd.notna(r.get("pp_price")) else 0.0, step=0.01)
                    epp4, epp5, epp6 = st.columns(3)
                    with epp4:
                        e_pp_securities_2 = st.number_input("PP Securities (2)", value=int(r["pp_securities_2"]) if pd.notna(r.get("pp_securities_2")) else 0, step=100_000)
                    with epp5:
                        e_pp_sec_type_2 = st.selectbox("PP Securities Type (2)", PP_SECURITY_TYPES, index=_idx(PP_SECURITY_TYPES, r.get("pp_securities_type_2") or ""))
                    with epp6:
                        e_pp_price_2 = st.number_input("PP Price (2) ($)", value=float(r["pp_price_2"]) if pd.notna(r.get("pp_price_2")) else 0.0, step=0.01)

                    st.markdown("**Other**")
                    e_notes = st.text_area("Notes", value=r.get("notes") or "")

                    if st.form_submit_button("Save Changes", type="primary"):
                        e_uw_1    = resolve_pick(e_uw_1_sel, e_uw_1_new)
                        e_uw_list = [u for u in [e_uw_1] + e_uw_others if u and u.strip()]

                        update = {
                            "company_name":           e_name,
                            "cik":                    e_cik or None,
                            "edgar_url":              e_edgar_url or None,
                            "ticker":                 e_ticker or None,
                            "ticker_units":           e_ticker_units or None,
                            "ticker_warrants":        e_ticker_warrants or None,
                            "ticker_rights":          e_ticker_rights or None,
                            "exchange":               e_exchange or None,
                            "auditor":                resolve_pick(e_auditor_sel, e_auditor_new) or None,
                            "auditor_since":          e_auditor_since or None,
                            "audit_report_date":      e_audit_report_date or None,
                            "audit_partner_id":       e_audit_partner_id or None,
                            "effective_date":         e_effective.isoformat() if e_effective else None,
                            "ipo_date":               e_ipo.isoformat() if e_ipo else None,
                            "offer_price":            e_offer or None,
                            "securities_type":        e_sec_type,
                            "securities_offered":     int(e_securities) if e_securities else None,
                            "warrant_count":          float(e_warrant_count) if e_warrant_count else None,
                            "warrant_strike_price":   e_warrant_strike or None,
                            "rights_count":           float(e_rights_count) if e_rights_count else None,
                            "overallotment_option":   int(e_oa_option) if e_oa_option else None,
                            "overallotment_exercised":     int(e_oa_exercised) if pd.notna(e_oa_exercised) else None,
                            "overallotment_exercised_date":e_oa_exercised_date.isoformat() if e_oa_exercised_date else None,
                            "pp_securities":          int(e_pp_securities) if e_pp_securities else None,
                            "pp_securities_type":     e_pp_sec_type or None,
                            "pp_price":               e_pp_price or None,
                            "pp_securities_2":        int(e_pp_securities_2) if e_pp_securities_2 else None,
                            "pp_securities_type_2":   e_pp_sec_type_2 or None,
                            "pp_price_2":             e_pp_price_2 or None,
                            "underwriters_list":      e_uw_list,
                            "notes":                  e_notes or None,
                            "image_url":              e_image or None,
                            "updated_at":             datetime.utcnow().isoformat(),
                        }
                        service_client().table("ipos").update(update).eq("id", sel_id).execute()
                        st.success("Saved!")
                        refresh()
                        st.rerun()

            with col_del:
                st.markdown("**Delete**")
                st.warning(f"Permanently delete **{r['company_name']}**?")
                if st.button("Delete", type="secondary", key="del_btn"):
                    service_client().table("ipos").delete().eq("id", sel_id).execute()
                    st.success("Deleted.")
                    refresh()
                    st.rerun()

            # ── Manage Filings ─────────────────────────────────────────────────
            st.divider()
            st.markdown(f"#### Filings — {r.get('company_name', '')}")

            filings = list(r.get("filings") or [])

            if filings:
                for i, f in enumerate(filings):
                    fc1, fc2, fc3, fc4 = st.columns([1, 1.5, 3, 0.7])
                    with fc1:
                        st.write(f.get("type", ""))
                    with fc2:
                        st.write(f.get("desc", ""))
                    with fc3:
                        url = f.get("url", "")
                        st.markdown(f"[{url}]({url})")
                    with fc4:
                        if st.button("Remove", key=f"rm_{sel_id}_{i}"):
                            filings.pop(i)
                            service_client().table("ipos").update({"filings": filings}).eq("id", sel_id).execute()
                            st.success("Removed.")
                            refresh()
                            st.rerun()
            else:
                st.caption("No filings added yet.")

            with st.form(f"add_filing_{sel_id}"):
                nf1, nf2, nf3 = st.columns([1, 1.5, 3])
                with nf1:
                    nf_type = st.selectbox("Type", FILING_TYPES)
                with nf2:
                    nf_desc = st.text_input("Label (optional)")
                with nf3:
                    nf_url = st.text_input("URL")
                if st.form_submit_button("Add Filing"):
                    if nf_url:
                        new_f = {"type": nf_type, "url": nf_url}
                        if nf_desc:
                            new_f["desc"] = nf_desc
                        filings.append(new_f)
                        service_client().table("ipos").update({"filings": filings}).eq("id", sel_id).execute()
                        st.success("Filing added!")
                        refresh()
                        st.rerun()
                    else:
                        st.error("URL is required.")

    # ── Verify 10-K ──────────────────────────────────────────────────────────
    # NOTE: requires a 'verified' boolean column (default false) in the ipos table
    with tab_verify:
        full_df_v = load_ipos()

        def _has_tenk(filings_val):
            if not filings_val:
                return False
            return any(isinstance(f, dict) and f.get("type") == "10-K" for f in filings_val)

        # Only show unverified records that have a 10-K filing
        df_to_verify = full_df_v[
            full_df_v["filings"].apply(_has_tenk) &
            ~full_df_v["verified"].fillna(False).astype(bool)
        ]

        if df_to_verify.empty:
            st.info("No unverified records with 10-K filings. Either all records are verified or none have a 10-K yet.")
        else:
            v_options = {
                f"{r['company_name']}  (ID {r['id']})": r["id"]
                for _, r in df_to_verify.sort_values("company_name").iterrows()
            }
            v_sel_label = st.selectbox("Select company", list(v_options.keys()), key="verify_select")
            v_id = v_options[v_sel_label]
            v_row = full_df_v[full_df_v["id"] == v_id].iloc[0]

            # Always use the 1st 10-K
            v_tenk_filings = [f for f in (v_row.get("filings") or []) if isinstance(f, dict) and f.get("type") == "10-K"]
            v_tenk_url = v_tenk_filings[0]["url"]
            v_tenk_label = "1st 10-K"

            st.link_button("View 1st 10-K 📄", v_tenk_url, use_container_width=True)

            vcol1, vcol2 = st.columns(2)
            with vcol1:
                run_verify = st.button("Run IPO Verification", key="run_verify_btn", use_container_width=True)
            with vcol2:
                is_verified = bool(v_row.get("verified"))
                mark_verified = st.button(
                    "✅ Mark as Verified",
                    key="mark_verified_btn",
                    type="primary",
                    disabled=is_verified,
                    use_container_width=True,
                )

            if mark_verified and not is_verified:
                try:
                    patch = {"verified": True}
                    # If OA was exercised but date is missing, fill with IPO date
                    if v_row.get("overallotment_exercised") and not v_row.get("overallotment_exercised_date") and v_row.get("ipo_date"):
                        patch["overallotment_exercised_date"] = v_row["ipo_date"]
                    # Also apply extracted OA date if it was found and stored is missing
                    if "verify_result" in st.session_state:
                        _vr = st.session_state["verify_result"]
                        if _vr[0] == v_id:
                            _ext_oa_date = _vr[2].get("overallotment_exercised_date")
                            if _ext_oa_date and not v_row.get("overallotment_exercised_date"):
                                patch["overallotment_exercised_date"] = _ext_oa_date
                    service_client().table("ipos").update(patch).eq("id", v_id).execute()
                    st.success(f"Marked {v_row['company_name']} as verified!")
                    refresh()
                    st.rerun()
                except Exception as e:
                    st.error(f"Could not save: {e}\n\nEnsure a 'verified' boolean column exists in the ipos table.")

            if run_verify:
                with st.spinner(f"Extracting from {v_tenk_label}…"):
                    _extracted = extract_from_10k(v_tenk_url)
                st.session_state["verify_result"] = (v_id, v_tenk_label, _extracted)

            if "verify_result" in st.session_state:
                _vid, _vlabel, _ext = st.session_state["verify_result"]
                if _vid == v_id and _vlabel == v_tenk_label:
                    _dbg = _ext.pop("_debug", {})
                    with st.expander("Debug info"):
                        st.write(f"**Anchor matched:** `{_dbg.get('anchor')}`  |  **Position:** {_dbg.get('idx')}")
                        st.write("**Section 12(b) extracted (first 400 chars):**")
                        st.code(_dbg.get("sec12b", ""), language=None)
                        st.write("**Excerpt start (first 300 chars):**")
                        st.code(_dbg.get("excerpt_start", ""), language=None)
                        st.write("**Claude raw response:**")
                        st.code(_dbg.get("claude_raw", ""), language=None)
                    if not _ext or all(v is None for v in _ext.values()):
                        st.error("Could not extract data — the 10-K may lack a structured IPO/PP section. Check debug info above.")
                    else:
                        st.markdown(f"#### Stored vs. {_vlabel}")

                        def _fmt_v(v):
                            if v is None or (isinstance(v, float) and pd.isna(v)):
                                return "—"
                            return str(v)

                        def _cmp(stored, extr):
                            if (stored is None or stored == "" or (isinstance(stored, float) and pd.isna(stored))) and \
                               (extr is None or extr == ""):
                                return "—"
                            if stored is None or extr is None:
                                return "⚠️"
                            try:
                                s, e = float(stored), float(extr)
                                return "✅" if abs(s - e) / max(abs(s), 1) < 0.01 else "❌"
                            except (ValueError, TypeError):
                                pass
                            try:
                                s_d = pd.to_datetime(str(stored)).date()
                                e_d = pd.to_datetime(str(extr)).date()
                                return "✅" if s_d == e_d else "❌"
                            except Exception:
                                pass
                            return "✅" if str(stored).strip().lower() == str(extr).strip().lower() else "❌"

                        comparisons = [
                            ("IPO Date",           v_row.get("ipo_date"),                     _ext.get("ipo_date")),
                            ("Securities Offered", v_row.get("securities_offered"),           _ext.get("securities_offered")),
                            ("OA Exercised",       v_row.get("overallotment_exercised"),      _ext.get("overallotment_exercised")),
                            ("OA Exercised Date",  v_row.get("overallotment_exercised_date"), _ext.get("overallotment_exercised_date")),
                            ("Ticker",             v_row.get("ticker"),                       _ext.get("ticker")),
                            ("Units Ticker",       v_row.get("ticker_units"),                 _ext.get("ticker_units")),
                            ("Warrant Ticker",     v_row.get("ticker_warrants"),              _ext.get("ticker_warrants")),
                            ("Rights Ticker",      v_row.get("ticker_rights"),                _ext.get("ticker_rights")),
                        ]

                        mismatches = 0
                        cmp_rows = []
                        for field, sv, ev in comparisons:
                            icon = _cmp(sv, ev)
                            if icon == "❌":
                                mismatches += 1
                            cmp_rows.append({"": icon, "Field": field, "Stored": _fmt_v(sv), "Extracted from 10-K": _fmt_v(ev)})

                        st.dataframe(pd.DataFrame(cmp_rows), hide_index=True, use_container_width=True)

                        if mismatches == 0:
                            st.success("All fields match — safe to mark as verified.")
                        else:
                            st.warning(f"{mismatches} field(s) differ. Review above before marking as verified.")

# ── Watchlist ─────────────────────────────────────────────────────────────────

st.divider()
st.subheader("Watchlist — Registered but Not Yet Consummated")
st.caption("SPACs that have filed but not yet completed their IPO.")

wdf = load_watchlist()

if not wdf.empty:
    w_display_cols = [c for c in ["company_name", "cik", "edgar_url", "s1_url", "notes", "created_at"] if c in wdf.columns]
    st.dataframe(
        wdf[w_display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "company_name": st.column_config.TextColumn("Company"),
            "cik":          st.column_config.TextColumn("CIK"),
            "edgar_url":    st.column_config.LinkColumn("EDGAR Page", display_text="View"),
            "s1_url":       st.column_config.LinkColumn("S-1", display_text="View"),
            "notes":        st.column_config.TextColumn("Notes"),
            "created_at":   st.column_config.DatetimeColumn("Added", format="MMM D, YYYY"),
        },
    )
    st.caption(f"{len(wdf)} entr{'y' if len(wdf) == 1 else 'ies'} on watchlist")
else:
    st.info("Watchlist is empty.")

if st.session_state.is_admin:
    w_col1, w_col2 = st.columns([3, 1])

    with w_col1:
        with st.form("add_watchlist", clear_on_submit=True):
            st.markdown("**Add to Watchlist**")
            wc1, wc2 = st.columns(2)
            with wc1:
                w_name      = st.text_input("Company Name *")
                w_cik       = st.text_input("CIK")
                w_edgar_url = st.text_input("EDGAR Homepage URL")
            with wc2:
                w_s1_url = st.text_input("S-1 URL")
                w_notes  = st.text_area("Notes", height=100)
            if st.form_submit_button("Add to Watchlist", type="primary"):
                if not w_name:
                    st.error("Company Name is required.")
                else:
                    service_client().table("watchlist").insert({
                        "company_name": w_name,
                        "cik":          w_cik or None,
                        "edgar_url":    w_edgar_url or None,
                        "s1_url":       w_s1_url or None,
                        "notes":        w_notes or None,
                    }).execute()
                    st.success(f"Added {w_name} to watchlist!")
                    refresh()
                    st.rerun()

    with w_col2:
        if not wdf.empty:
            st.markdown("**Remove Entry**")
            w_options = {
                f"{r['company_name']} (ID {r['id']})": r["id"]
                for _, r in wdf.iterrows()
            }
            w_sel_label = st.selectbox("Select", list(w_options.keys()), key="w_del_select", label_visibility="collapsed")
            w_sel_id    = w_options[w_sel_label]
            if st.button("Remove", type="secondary", key="w_del_btn"):
                service_client().table("watchlist").delete().eq("id", w_sel_id).execute()
                st.success("Removed.")
                refresh()
                st.rerun()

# ── SPAC Audit Partners ────────────────────────────────────────────────────────

st.divider()
st.subheader("SPAC Audit Partners")
st.caption("Audit engagement partners linked to SPACs in this database, sourced from PCAOB Form AP filings.")

# Build display: join ipos audit_partner_id → pcaob_partners
_ipos_all = load_ipos()
_ipos_with_pid = _ipos_all[_ipos_all["audit_partner_id"].notna()][["company_name", "audit_partner_id", "ipo_date"]].copy()

if _ipos_with_pid.empty:
    st.info("No SPACs in the database have an Audit Partner ID assigned yet.")
else:
    _partner_summary = (
        _ipos_with_pid
        .groupby("audit_partner_id")
        .agg(
            spac_count=("company_name", "count"),
            companies=("company_name", lambda x: ", ".join(sorted(x))),
            years=("ipo_date", lambda x: ", ".join(
                sorted(set(str(pd.to_datetime(v).year) for v in x if pd.notna(v)))
            )),
        )
        .reset_index()
    )

    _pcaob = load_spac_audit_partners()
    if not _pcaob.empty:
        _pcaob_cols = ["engagement_partner_id", "first_name", "middle_name", "last_name", "suffix"]
        if "firm_name" in _pcaob.columns:
            _pcaob_cols.append("firm_name")
        _merged = _partner_summary.merge(
            _pcaob[_pcaob_cols],
            left_on="audit_partner_id",
            right_on="engagement_partner_id",
            how="left",
        )

        def _full_name(r):
            parts = [r.get("first_name"), r.get("middle_name"), r.get("last_name"), r.get("suffix")]
            name = " ".join(p for p in parts if pd.notna(p) and str(p).strip())
            return name if name else f"ID: {r['audit_partner_id']}"

        _merged["partner_name"] = _merged.apply(_full_name, axis=1)
        _disp_cols = ["partner_name", "audit_partner_id"]
        if "firm_name" in _merged.columns:
            _disp_cols.append("firm_name")
        _disp_cols += ["years", "spac_count", "companies"]
        _display = _merged[_disp_cols].sort_values("spac_count", ascending=False)
        st.dataframe(
            _display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "partner_name":     st.column_config.TextColumn("Partner Name"),
                "audit_partner_id": st.column_config.TextColumn("Partner ID"),
                "firm_name":        st.column_config.TextColumn("Firm"),
                "years":            st.column_config.TextColumn("Year(s)"),
                "spac_count":       st.column_config.NumberColumn("# SPACs", format="%.0f"),
                "companies":        st.column_config.TextColumn("Companies"),
            },
        )
    else:
        # PCAOB table not populated yet — show IDs and counts only
        _partner_summary_display = _partner_summary.sort_values("spac_count", ascending=False)
        st.dataframe(
            _partner_summary_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "audit_partner_id": st.column_config.TextColumn("Partner ID"),
                "years":            st.column_config.TextColumn("Year(s)"),
                "spac_count":       st.column_config.NumberColumn("# SPACs", format="%.0f"),
                "companies":        st.column_config.TextColumn("Companies"),
            },
        )
        st.caption("Partner names not yet loaded. Use the Refresh button below to populate from PCAOB.")

if st.session_state.is_admin:
    st.markdown("**Refresh PCAOB Partner Data**")
    st.caption("Downloads Form AP data from PCAOB and upserts unique engagement partners into the database. Run monthly.")
    if st.button("Refresh PCAOB Data", key="refresh_pcaob"):
        PCAOB_ZIP_URL = "https://pcaobus.org/assets/PCAOBFiles/FirmFilings.zip"
        with st.spinner("Downloading PCAOB Form AP data…"):
            try:
                r = requests.get(PCAOB_ZIP_URL, timeout=120)
                r.raise_for_status()
            except Exception as e:
                st.error(f"Download failed: {e}")
                st.stop()

        with st.spinner("Parsing ZIP…"):
            try:
                with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                    csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if not csv_files:
                        st.error("No CSV file found inside the ZIP.")
                        st.stop()
                    # Use the largest CSV (the main data file)
                    csv_name = max(csv_files, key=lambda n: zf.getinfo(n).file_size)
                    with zf.open(csv_name) as f:
                        try:
                            df_raw = pd.read_csv(f, dtype=str, encoding="utf-8")
                        except UnicodeDecodeError:
                            df_raw = pd.read_csv(f, dtype=str, encoding="latin-1")
            except Exception as e:
                st.error(f"Failed to parse ZIP: {e}")
                st.stop()

        REQUIRED = [
            "Engagement Partner ID",
            "Engagement Partner Last Name",
            "Engagement Partner First Name",
            "Engagement Partner Middle Name",
            "Engagement Partner Suffix",
        ]
        OPTIONAL_FIRM = "Firm Name"
        has_firm = OPTIONAL_FIRM in df_raw.columns
        missing = [c for c in REQUIRED if c not in df_raw.columns]
        if missing:
            st.error(f"Expected columns not found in CSV: {missing}")
            st.stop()

        with st.spinner("Deduplicating and upserting…"):
            cols = REQUIRED + ([OPTIONAL_FIRM] if has_firm else [])
            df_partners = (
                df_raw[cols]
                .dropna(subset=["Engagement Partner ID"])
                .drop_duplicates(subset=["Engagement Partner ID"])
                .copy()
            )
            col_names = ["engagement_partner_id", "last_name", "first_name", "middle_name", "suffix"]
            if has_firm:
                col_names.append("firm_name")
            df_partners.columns = col_names
            # Replace NaN with None for JSON serialisation
            df_partners = df_partners.astype(object).where(pd.notna(df_partners), None)
            now_str = datetime.utcnow().isoformat()
            df_partners["updated_at"] = now_str

            rows = df_partners.to_dict(orient="records")
            BATCH = 500
            for i in range(0, len(rows), BATCH):
                service_client().table("pcaob_partners").upsert(
                    rows[i : i + BATCH],
                    on_conflict="engagement_partner_id",
                ).execute()

        st.success(f"Done — {len(rows):,} unique engagement partners loaded.")
        refresh()
        st.rerun()
