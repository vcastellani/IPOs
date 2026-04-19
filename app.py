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
FILING_TYPES = ["S-1", "S-1/A", "8-K (IPO)", "8-K (Combination)", "424B4", "Other"]

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

    # Find auditor section using multiple patterns
    auditor_section = ""
    for pat in [
        r'REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM',
        r'served as the Company',
        r'EXPERTS',
        r'CERTAIN LEGAL MATTERS',
        r'audited by',
        r'independent registered public accounting firm',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            start = max(0, m.start() - 300)
            auditor_section = text[start:start + 10000]
            break
    # Always include a large tail in case section is near the very end
    tail = text[-8000:]
    excerpt = text[:15000] + "\n\n[...]\n\n" + auditor_section + "\n\n[end of doc]\n\n" + tail



    msg = anthropic_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=[{
            "type": "text",
            "text": "You are a financial document parser for SEC filings. Return only valid JSON, no markdown.",
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[
            {"role": "user", "content": f"""Extract these fields from the SPAC 424B4 prospectus. Return ONLY a raw JSON object with no explanation:

{{
  "company_name": "full legal company name",
  "securities_offered": 12500000,
  "securities_type": "Units - Shares and Warrants",
  "auditor": "Audit firm name",
  "auditor_since": 2021,
  "overallotment_option": 1875000,
  "underwriters": ["Lead Underwriter", "Co-Underwriter"],
  "warrant_count": 0.5,
  "warrant_strike_price": 11.50,
  "rights_count": 0.1
}}

Rules:
- securities_type must be exactly one of: "Shares", "Units - Shares and Warrants", "Units - Shares and Rights", "Units - Shares, Warrants, and Rights"
- securities_offered is the integer share/unit count (not a dollar amount)
- warrant_count is warrants per unit (e.g. 0.5), null if not applicable
- rights_count: IMPORTANT - express as rights PER UNIT as a decimal. "one right per unit" = 1.0. "one right for every 5 units" or "1/5 of one right" or "one right per five units" = 0.2. "one-half of one right" = 0.5. Do NOT return 1 if the unit contains a fractional right - calculate the decimal carefully. null if no rights.
- warrant_strike_price is the exercise price in dollars, null if not applicable
- auditor: find the "/s/ Firm Name" signature line near the end of the "REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM" section; the firm name repeats on the next line and may be followed by a website URL (e.g. www.malonebailey.com) - ignore the URL, use only the firm name exactly as written after "/s/" (e.g. "MaloneBailey, LLP", "Marcum llp", "WithumSmith+Brown, PC")
- auditor_since: integer year from phrases like "We have served as the Company's auditor since YYYY" or "auditor since inception" - null if not found
- overallotment_option: integer share/unit count the underwriters have the option to purchase (e.g. "45-day option to purchase up to X additional units") - null if not found
- underwriters: lead underwriter first, null if not found

Filing text:
{excerpt}"""},
            {"role": "assistant", "content": "{{"},
        ],


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

    effect_dt    = _date.fromisoformat(effect_date)
    window_start = effect_dt - timedelta(days=3)
    window_end   = effect_dt + timedelta(days=21)

    prospectus_url = None
    s1_url         = None
    s1_date        = None

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

    if prospectus_url is None:
        raise ValueError(f"No 424B4 or 424B3 found for CIK {cik} within 3 days before / 21 days after {effect_date}")
    return {"prospectus_url": prospectus_url, "s1_url": s1_url}

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

st.header("SPAC IPOs")

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

    display_cols = [c for c in ["company_name", "prospectus_url", "cik", "ticker", "size_m"] if c in df.columns]

    st.dataframe(
        df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "company_name":   st.column_config.TextColumn("Company"),
            "prospectus_url": st.column_config.LinkColumn("Prospectus", display_text="📄"),
            "cik":            st.column_config.TextColumn("CIK"),
            "ticker":         st.column_config.TextColumn("Ticker"),
            "size_m":         st.column_config.NumberColumn("Size ($M)", format="$ %.1f"),
        },
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
    years_available = sorted(df_dated["ipo_date"].dt.year.unique().tolist(), reverse=True)
    if years_available:
        sel_year = st.selectbox("Year", years_available, key="chart_year")
        df_year  = df_dated[df_dated["ipo_date"].dt.year == sel_year]

        MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        monthly = (
            df_year.groupby(df_year["ipo_date"].dt.month)
            .size()
            .reindex(range(1, 13), fill_value=0)
        )
        monthly.index = MONTH_NAMES
        monthly.name  = "SPAC IPOs"

        st.markdown(f"**SPAC IPOs by Month — {sel_year}**")
        st.bar_chart(monthly, y_label="# of IPOs", x_label="Month")
        st.caption(f"{len(df_year)} IPO(s) in {sel_year}")
    st.divider()

# ── Detail view ───────────────────────────────────────────────────────────────

if not df.empty:
    with st.expander("Detail View"):
        names  = df["company_name"].tolist()
        chosen = st.selectbox("Select a company", names, key="detail_select")
        if chosen:
            row = df[df["company_name"] == chosen].iloc[0]
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
                    "Exchange":             row.get("exchange"),
                    "Auditor":              row.get("auditor"),
                    "Auditor Since":        row.get("auditor_since"),
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
    tab_add, tab_edit = st.tabs(["Add New Entry", "Edit / Delete"])

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
                            data["cik"] = f"{int(pf_cik):010d}"
                            data["effective_date"] = pf_date.isoformat()
                            cik_int = int(pf_cik)
                            data["edgar_url"] = f"https://www.sec.gov/edgar/browse/?CIK={cik_int:010d}"
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
        st.divider()

        with st.form("add_form", clear_on_submit=True):
            st.markdown("**Initial Filings**")
            fi1, fi2, fi3 = st.columns(3)
            with fi1:
                a_s1_url = st.text_input("S-1 URL", value=pf.get("s1_url") or "")
            with fi2:
                a_8k_url = st.text_input("8-K URL")
            with fi3:
                a_prospectus_url = st.text_input("Prospectus (424B4) URL", value=pf.get("prospectus_url", ""))


            c1, c2, c3 = st.columns(3)

            with c1:
                st.markdown("**Company**")
                a_name = st.text_input("Company Name *", value=pf.get("company_name", ""))
                a_cik           = st.text_input("CIK", value=pf.get("cik", ""))
                a_edgar_url     = st.text_input("EDGAR Homepage URL", value=pf.get("edgar_url", ""))
                a_ticker        = st.text_input("Common Stock Ticker")
                a_exchange      = st.selectbox("Exchange", EXCHANGES)
                _known_aud    = load_known_auditors()
                _aud_opts     = [""] + _known_aud + ["Other / New..."]
                pf_auditor_raw = pf.get("auditor") or ""
                _aud_matched   = _fuzzy_match(pf_auditor_raw, _known_aud)
                pf_auditor     = _aud_matched if _aud_matched else pf_auditor_raw
                _aud_idx       = _aud_opts.index(pf_auditor) if pf_auditor in _known_aud else (len(_aud_opts) - 1 if pf_auditor else 0)
                a_auditor_sel = st.selectbox("Auditor", _aud_opts, index=_aud_idx)
                a_auditor_new = st.text_input("New auditor name", value=pf_auditor if pf_auditor not in _known_aud else "", placeholder="Type if not listed above")
                a_auditor_since     = st.text_input("Auditor Since", value=str(pf.get("auditor_since", "")) if pf.get("auditor_since") else "")
                a_audit_partner_id  = st.text_input("Audit Partner ID")
                a_image             = st.text_input("Image URL")

            with c2:
                st.markdown("**Dates & Pricing**")
                a_effective = pf.get("effective_date")
                a_ipo       = st.date_input("IPO Date", value=None)

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
                a_oa_exercised      = st.number_input("Exercised (securities)", min_value=0, step=100_000, value=None)
                a_oa_exercised_date = st.date_input("Exercise Date", value=None, key="add_oa_ex_date")

            st.markdown("**Private Placement**")
            pp1, pp2, pp3 = st.columns(3)
            with pp1:
                a_pp_securities = st.number_input("PP Securities (1)", min_value=0, step=100_000, value=None)
            with pp2:
                a_pp_sec_type = st.selectbox("PP Securities Type (1)", PP_SECURITY_TYPES)
            with pp3:
                a_pp_price = st.number_input("PP Price (1) ($)", min_value=0.0, step=0.01, value=None)
            pp4, pp5, pp6 = st.columns(3)
            with pp4:
                a_pp_securities_2 = st.number_input("PP Securities (2)", min_value=0, step=100_000, value=None)
            with pp5:
                a_pp_sec_type_2 = st.selectbox("PP Securities Type (2)", PP_SECURITY_TYPES)
            with pp6:
                a_pp_price_2 = st.number_input("PP Price (2) ($)", min_value=0.0, step=0.01, value=None)

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

                    new_row = {
                        "company_name":           a_name,
                        "cik":                    a_cik or None,
                        "edgar_url":              a_edgar_url or None,
                        "ticker":                 a_ticker or None,
                        "exchange":               a_exchange or None,
                        "auditor":                resolve_pick(a_auditor_sel, a_auditor_new) or None,
                        "auditor_since":          a_auditor_since or None,
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
                        e_ticker        = st.text_input("Common Stock Ticker", value=r.get("ticker") or "")
                        e_exchange      = st.selectbox("Exchange", EXCHANGES, index=_idx(EXCHANGES, r.get("exchange") or ""))
                        _known_aud_e   = load_known_auditors()
                        _existing_aud  = r.get("auditor") or ""
                        _aud_opts_e    = [""] + _known_aud_e + ["Other / New..."]
                        _aud_idx       = _aud_opts_e.index(_existing_aud) if _existing_aud in _known_aud_e else (len(_aud_opts_e) - 1 if _existing_aud else 0)
                        e_auditor_sel  = st.selectbox("Auditor", _aud_opts_e, index=_aud_idx)
                        e_auditor_new  = st.text_input("New auditor name", value=_existing_aud if _existing_aud not in _known_aud_e else "", placeholder="Type if not listed above")
                        e_auditor_since    = st.text_input("Auditor Since", value=r.get("auditor_since") or "")
                        e_audit_partner_id = st.text_input("Audit Partner ID", value=r.get("audit_partner_id") or "")
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
                            "exchange":               e_exchange or None,
                            "auditor":                resolve_pick(e_auditor_sel, e_auditor_new) or None,
                            "auditor_since":          e_auditor_since or None,
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
