import streamlit as st
from supabase import create_client, Client
import pandas as pd
from datetime import date, datetime, timedelta

st.set_page_config(
    page_title="IPO & SPAC Tracker",
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
FILING_TYPES = ["S-1", "S-1/A", "8-K (IPO)", "8-K (Combination)", "Other"]

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

# ── Session state ─────────────────────────────────────────────────────────────

if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📈 IPO & SPAC Tracker")
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

st.header("IPO & SPAC Tracker")

df = load_ipos()

if not df.empty:
    if filter_exchange:
        df = df[df["exchange"].isin(filter_exchange)]
    if search:
        df = df[df["company_name"].str.contains(search, case=False, na=False)]

    if "offer_price" in df.columns and "securities_offered" in df.columns:
        df["computed_total"] = (
            df["offer_price"].fillna(0) * df["securities_offered"].fillna(0) / 1_000_000
        ).round(1)

    display_cols = [c for c in [
        "company_name", "ticker", "exchange", "effective_date", "ipo_date",
        "offer_price", "securities_type", "securities_offered", "computed_total",
    ] if c in df.columns]

    st.dataframe(
        df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "company_name":       st.column_config.TextColumn("Company"),
            "ticker":             st.column_config.TextColumn("Common Stock Ticker"),
            "exchange":           st.column_config.TextColumn("Exchange"),
            "effective_date":     st.column_config.DateColumn("Effective Date"),
            "ipo_date":           st.column_config.DateColumn("IPO Date"),
            "offer_price":        st.column_config.NumberColumn("Price", format="$,.2f"),
            "securities_type":    st.column_config.TextColumn("Securities Type"),
            "securities_offered": st.column_config.NumberColumn("Securities Offered", format=",.0f"),
            "computed_total":     st.column_config.NumberColumn("Total ($M)", format="$,.1f"),
        },
    )
    st.caption(f"{len(df)} filing(s) shown")
else:
    st.info("No filings yet. Log in as admin to add entries.")

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
                if row.get("ipo_date") and row.get("overallotment_days"):
                    oa_date = pd.to_datetime(row["ipo_date"]).date() + timedelta(days=int(row["overallotment_days"]))
                    oa_date_str = oa_date.strftime("%B %d, %Y")

                total_str = None
                if row.get("offer_price") and row.get("securities_offered"):
                    total = row["offer_price"] * row["securities_offered"] / 1_000_000
                    total_str = f"${total:,.1f}M"

                oa_stat = oa_status(row.get("overallotment_option"), row.get("overallotment_exercised"))

                fields = {
                    "CIK":                  row.get("cik"),
                    "Common Stock Ticker":  row.get("ticker"),
                    "Exchange":             row.get("exchange"),
                    "Auditor":              row.get("auditor"),
                    "Auditor Since":        row.get("auditor_since"),
                    "Effective Date":       row.get("effective_date"),
                    "IPO Date":             row.get("ipo_date"),
                    "Price":                f"${row['offer_price']:,.2f}" if row.get("offer_price") else None,
                    "Securities Type":      row.get("securities_type"),
                    "Securities Offered":   fmt_int(row.get("securities_offered")),
                    "Total Offering":       total_str,
                    "Warrants":             f"{fmt_warrants(row.get('warrant_count'))} @ ${row['warrant_strike_price']:,.2f}" if row.get("warrant_count") else None,
                    "Rights":               fmt_int(row.get("rights_count")),
                    "Overallotment Option": f"{fmt_int(row.get('overallotment_option'))} securities" if row.get("overallotment_option") else None,
                    "Overallotment Period": f"{int(row['overallotment_days'])} days" if row.get("overallotment_days") else None,
                    "Overallotment Expiry": oa_date_str,
                    "Overallotment Status": oa_stat,
                    "PP Securities":        fmt_int(row.get("pp_securities")),
                    "PP Securities Type":   row.get("pp_securities_type"),
                    "PP Price":             f"${row['pp_price']:,.2f}" if row.get("pp_price") else None,
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
                    st.image(img, use_container_width=True)

# ── Admin panel ───────────────────────────────────────────────────────────────

if st.session_state.is_admin:
    st.divider()
    st.subheader("Admin Panel")
    tab_add, tab_edit = st.tabs(["Add New Entry", "Edit / Delete"])

    # ── Add ───────────────────────────────────────────────────────────────────
    with tab_add:
        # Outside-form selectors for instant reactivity
        sel_col1, sel_col2 = st.columns(2)
        with sel_col1:
            st.markdown("##### Securities Type")
            a_sec_type     = st.selectbox("Securities Type", SECURITY_TYPES, key="add_sec_type", label_visibility="collapsed")
            a_has_warrants = "Warrant" in a_sec_type
            a_has_rights   = "Right"   in a_sec_type
        with sel_col2:
            st.markdown("##### Underwriters")
            a_uw_mode = st.radio("Underwriter count", ["Solo", "Multiple"], horizontal=True, key="add_uw_mode")

        with st.form("add_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)

            with c1:
                st.markdown("**Company**")
                a_name          = st.text_input("Company Name *")
                a_cik           = st.text_input("CIK")
                a_ticker        = st.text_input("Common Stock Ticker")
                a_exchange      = st.selectbox("Exchange", EXCHANGES)
                a_auditor       = st.text_input("Auditor")
                a_auditor_since = st.text_input("Auditor Since")
                a_image         = st.text_input("Image URL")

            with c2:
                st.markdown("**Dates & Pricing**")
                a_effective = st.date_input("Effective Date", value=None)
                a_ipo       = st.date_input("IPO Date", value=None)
                a_offer     = st.number_input("Price ($)", min_value=0.0, step=0.01, value=None)

                st.markdown("**Underwriters**")
                if a_uw_mode == "Solo":
                    a_uw_1      = st.text_input("Underwriter")
                    a_uw_others = []
                else:
                    uwc1, uwc2 = st.columns(2)
                    with uwc1:
                        a_uw_1 = st.text_input("Underwriter 1 (Lead)")
                        a_uw_3 = st.text_input("Underwriter 3")
                        a_uw_5 = st.text_input("Underwriter 5")
                    with uwc2:
                        a_uw_2 = st.text_input("Underwriter 2")
                        a_uw_4 = st.text_input("Underwriter 4")
                        a_uw_6 = st.text_input("Underwriter 6")
                    a_uw_others = [a_uw_2, a_uw_3, a_uw_4, a_uw_5, a_uw_6]

            with c3:
                st.markdown("**Securities**")
                a_securities = st.number_input("Securities Offered", min_value=0, step=100_000, value=None)

                if a_has_warrants:
                    a_warrant_count  = st.number_input("Number of Warrants", min_value=0.0, step=0.5, value=None)
                    a_warrant_strike = st.number_input("Warrant Strike Price ($)", min_value=0.0, step=0.01, value=None)
                else:
                    a_warrant_count  = None
                    a_warrant_strike = None

                if a_has_rights:
                    a_rights_count = st.number_input("Number of Rights", min_value=0, step=100_000, value=None)
                else:
                    a_rights_count = None

                st.markdown("**Overallotment**")
                a_oa_option    = st.number_input("Total Option (securities)", min_value=0, step=100_000, value=None)
                a_oa_days      = st.number_input("Option Period (days)", min_value=0, step=1, value=None)
                a_oa_exercised = st.number_input("Exercised (securities)", min_value=0, step=100_000, value=None)

            st.markdown("**Private Placement**")
            pp1, pp2, pp3 = st.columns(3)
            with pp1:
                a_pp_securities = st.number_input("PP Securities", min_value=0, step=100_000, value=None)
            with pp2:
                a_pp_sec_type = st.selectbox("PP Securities Type", PP_SECURITY_TYPES)
            with pp3:
                a_pp_price = st.number_input("PP Price ($)", min_value=0.0, step=0.01, value=None)

            st.markdown("**Other**")
            a_notes = st.text_area("Notes")

            st.markdown("**Initial Filings**")
            fi1, fi2 = st.columns(2)
            with fi1:
                a_s1_url = st.text_input("S-1 URL")
            with fi2:
                a_8k_url = st.text_input("8-K URL")

            if st.form_submit_button("Add Entry", type="primary"):
                if not a_name:
                    st.error("Company Name is required.")
                else:
                    uw_list = [u for u in [a_uw_1] + a_uw_others if u and u.strip()]

                    initial_filings = []
                    if a_s1_url:
                        initial_filings.append({"type": "S-1", "url": a_s1_url})
                    if a_8k_url:
                        initial_filings.append({"type": "8-K (IPO)", "url": a_8k_url})

                    new_row = {
                        "company_name":           a_name,
                        "cik":                    a_cik or None,
                        "ticker":                 a_ticker or None,
                        "exchange":               a_exchange or None,
                        "auditor":                a_auditor or None,
                        "auditor_since":          a_auditor_since or None,
                        "effective_date":         a_effective.isoformat() if a_effective else None,
                        "ipo_date":               a_ipo.isoformat() if a_ipo else None,
                        "offer_price":            a_offer,
                        "securities_type":        a_sec_type,
                        "securities_offered":     int(a_securities) if a_securities else None,
                        "warrant_count":          float(a_warrant_count) if a_warrant_count else None,
                        "warrant_strike_price":   a_warrant_strike,
                        "rights_count":           int(a_rights_count) if a_rights_count else None,
                        "overallotment_option":   int(a_oa_option) if a_oa_option else None,
                        "overallotment_days":     int(a_oa_days) if a_oa_days else None,
                        "overallotment_exercised":int(a_oa_exercised) if a_oa_exercised is not None else None,
                        "pp_securities":          int(a_pp_securities) if a_pp_securities else None,
                        "pp_securities_type":     a_pp_sec_type or None,
                        "pp_price":               a_pp_price,
                        "underwriters_list":      uw_list,
                        "notes":                  a_notes or None,
                        "image_url":              a_image or None,
                        "filings":                initial_filings,
                    }
                    service_client().table("ipos").insert(new_row).execute()
                    st.success(f"Added {a_name}!")
                    refresh()
                    st.rerun()

    # ── Edit / Delete ─────────────────────────────────────────────────────────
    with tab_edit:
        full_df = load_ipos()
        if full_df.empty:
            st.info("No entries to edit yet.")
        else:
            options = {
                f"{r['company_name']}  (ID {r['id']})": r["id"]
                for _, r in full_df.iterrows()
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
                        e_ticker        = st.text_input("Common Stock Ticker", value=r.get("ticker") or "")
                        e_exchange      = st.selectbox("Exchange", EXCHANGES, index=_idx(EXCHANGES, r.get("exchange") or ""))
                        e_auditor       = st.text_input("Auditor", value=r.get("auditor") or "")
                        e_auditor_since = st.text_input("Auditor Since", value=r.get("auditor_since") or "")
                        e_image         = st.text_input("Image URL", value=r.get("image_url") or "")

                    with ec2:
                        st.markdown("**Dates & Pricing**")
                        e_effective = st.date_input("Effective Date", value=pd.to_datetime(r["effective_date"]).date() if r.get("effective_date") else None)
                        e_ipo       = st.date_input("IPO Date",       value=pd.to_datetime(r["ipo_date"]).date()       if r.get("ipo_date")       else None)
                        e_offer     = st.number_input("Price ($)", value=float(r["offer_price"]) if r.get("offer_price") else 0.0, step=0.01)

                        st.markdown("**Underwriters**")
                        uw0 = existing_uws[0] if len(existing_uws) > 0 else ""
                        if e_uw_mode == "Solo":
                            e_uw_1      = st.text_input("Underwriter", value=uw0)
                            e_uw_others = []
                        else:
                            uwc1, uwc2 = st.columns(2)
                            with uwc1:
                                e_uw_1 = st.text_input("Underwriter 1 (Lead)", value=uw0)
                                e_uw_3 = st.text_input("Underwriter 3", value=existing_uws[2] if len(existing_uws) > 2 else "")
                                e_uw_5 = st.text_input("Underwriter 5", value=existing_uws[4] if len(existing_uws) > 4 else "")
                            with uwc2:
                                e_uw_2 = st.text_input("Underwriter 2", value=existing_uws[1] if len(existing_uws) > 1 else "")
                                e_uw_4 = st.text_input("Underwriter 4", value=existing_uws[3] if len(existing_uws) > 3 else "")
                                e_uw_6 = st.text_input("Underwriter 6", value=existing_uws[5] if len(existing_uws) > 5 else "")
                            e_uw_others = [e_uw_2, e_uw_3, e_uw_4, e_uw_5, e_uw_6]

                    with ec3:
                        st.markdown("**Securities**")
                        e_securities = st.number_input("Securities Offered", value=int(r["securities_offered"]) if r.get("securities_offered") else 0, step=100_000)

                        if e_has_warrants:
                            e_warrant_count  = st.number_input("Number of Warrants", value=float(r["warrant_count"]) if r.get("warrant_count") else 0.0, step=0.5)
                            e_warrant_strike = st.number_input("Warrant Strike Price ($)", value=float(r["warrant_strike_price"]) if r.get("warrant_strike_price") else 0.0, step=0.01)
                        else:
                            e_warrant_count  = None
                            e_warrant_strike = None

                        if e_has_rights:
                            e_rights_count = st.number_input("Number of Rights", value=int(r["rights_count"]) if r.get("rights_count") else 0, step=100_000)
                        else:
                            e_rights_count = None

                        st.markdown("**Overallotment**")
                        e_oa_option    = st.number_input("Total Option (securities)", value=int(r["overallotment_option"]) if r.get("overallotment_option") else 0, step=100_000)
                        e_oa_days      = st.number_input("Option Period (days)", value=int(r["overallotment_days"]) if r.get("overallotment_days") else 0, step=1)
                        e_oa_exercised = st.number_input("Exercised (securities)", value=int(r["overallotment_exercised"]) if pd.notna(r.get("overallotment_exercised")) else 0, step=100_000)


                    st.markdown("**Private Placement**")
                    epp1, epp2, epp3 = st.columns(3)
                    with epp1:
                        e_pp_securities = st.number_input("PP Securities", value=int(r["pp_securities"]) if r.get("pp_securities") else 0, step=100_000)
                    with epp2:
                        e_pp_sec_type = st.selectbox("PP Securities Type", PP_SECURITY_TYPES, index=_idx(PP_SECURITY_TYPES, r.get("pp_securities_type") or ""))
                    with epp3:
                        e_pp_price = st.number_input("PP Price ($)", value=float(r["pp_price"]) if r.get("pp_price") else 0.0, step=0.01)

                    st.markdown("**Other**")
                    e_notes = st.text_area("Notes", value=r.get("notes") or "")

                    if st.form_submit_button("Save Changes", type="primary"):
                        e_uw_list = [u for u in [e_uw_1] + e_uw_others if u and u.strip()]
                        update = {
                            "company_name":           e_name,
                            "cik":                    e_cik or None,
                            "ticker":                 e_ticker or None,
                            "exchange":               e_exchange or None,
                            "auditor":                e_auditor or None,
                            "auditor_since":          e_auditor_since or None,
                            "effective_date":         e_effective.isoformat() if e_effective else None,
                            "ipo_date":               e_ipo.isoformat() if e_ipo else None,
                            "offer_price":            e_offer or None,
                            "securities_type":        e_sec_type,
                            "securities_offered":     int(e_securities) if e_securities else None,
                            "warrant_count":          float(e_warrant_count) if e_warrant_count else None,
                            "warrant_strike_price":   e_warrant_strike or None,
                            "rights_count":           int(e_rights_count) if e_rights_count else None,
                            "overallotment_option":   int(e_oa_option) if e_oa_option else None,
                            "overallotment_exercised":int(e_oa_exercised) if pd.notna(e_oa_exercised) else None,
                            "overallotment_exercised":int(e_oa_exercised) if e_oa_exercised is not None else None,
                            "pp_securities":          int(e_pp_securities) if e_pp_securities else None,
                            "pp_securities_type":     e_pp_sec_type or None,
                            "pp_price":               e_pp_price or None,
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
