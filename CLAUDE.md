# SPAC IPO Tracker — Project Context

## What this is
A Streamlit app that tracks Special Purpose Acquisition Company (SPAC) IPOs.
Public users browse verified SPACs; Vincent (admin) adds, edits, and verifies records.
A GitHub Actions job emails a daily digest of new SPAC EFFECT filings from EDGAR.

**Live app:** Streamlit Cloud, connected to Supabase.
**Repo:** `vcastellani/ipos`
**Dev branch:** `claude/setup-github-connection-SK276` — all changes go here, never push to main directly.

---

## Files

| File | Purpose |
|------|---------|
| `app.py` | Entire Streamlit app (~1800 lines) |
| `edgar_scraper.py` | Daily EDGAR EFFECT scraper + email sender |
| `.github/workflows/monthly_edgar_pull.yml` | GitHub Actions workflow (runs 1st of month, 9:30 AM ET; also manually dispatchable) |
| `requirements.txt` | `streamlit`, `supabase`, `pandas`, `requests`, `anthropic` |

---

## Database (Supabase)

**Tables:**
- `ipos` — one row per SPAC IPO (see key columns below)
- `watchlist` — Vincent's personal watchlist of SPACs
- `pcaob_partners` — PCAOB engagement partner records linked to IPOs

**Key `ipos` columns:**

| Column | Type | Notes |
|--------|------|-------|
| `id` | int | PK |
| `company_name` | text | |
| `cik` | text | SEC CIK (no leading zeros) |
| `ipo_date` | date | |
| `size_m` | float | IPO size in $M |
| `prospectus_url` | text | 424B4 URL |
| `verified` | boolean | default false — must exist in Supabase |
| `ticker` | text | Common stock ticker |
| `ticker_units` | text | |
| `ticker_warrants` | text | |
| `ticker_rights` | text | |
| `exchange` | text | NYSE / NASDAQ / AMEX |
| `securities_offered` | int | Base IPO count (excluding OA) |
| `overallotment_option` | int | Max OA units underwriters could buy |
| `overallotment_exercised` | int | Actual OA units purchased |
| `overallotment_exercised_date` | date | |
| `offer_price` | float | |
| `pp_securities` | int | Private placement count (1st type) |
| `pp_securities_type` | text | Shares / Warrants / Units – … |
| `pp_price` | float | |
| `pp_securities_2` / `pp_securities_type_2` / `pp_price_2` | | Second PP tranche if any |
| `auditor` | text | |
| `audit_report_date` | text | "Month DD, YYYY" format |
| `audit_partner_id` | text | PCAOB engagement partner ID |
| `underwriters_list` | jsonb | Array of underwriter name strings |
| `filings` | jsonb | Array of `{type, url, date}` filing objects |

**Supabase clients:**
- `anon_client()` — cached `@st.cache_resource`, used for all reads
- `service_client()` — uncached, used for all writes (requires `service_role_key`)

**Secrets** (in Streamlit secrets / `st.secrets`):
```toml
[supabase]
url = "..."
anon_key = "..."
service_role_key = "..."

anthropic_api_key = "..."
```

---

## App layout (`app.py`)

### Public sections
- **Sidebar** — admin password gate (`st.secrets["admin_password"]`)
- **Main table** — verified SPACs only: Name / CIK / IPO Date / Size / Prospectus / Verified badge; sorted by date descending
- **Analytics** — bar chart of IPOs per year
- **Detail view** — click a row → expandable card with all fields; sorted alphabetically by company name
- **Watchlist** — Vincent's personal list

### Admin panel (password-protected)
Three tabs: **Add New Entry** | **Edit / Delete** | **IPO Verification**

---

## Data ingestion pipeline

When a new SPAC is added via **Add New Entry**, the flow is:

1. **`find_edgar_urls(cik, effect_date)`** — calls `data.sec.gov/submissions/CIK{}.json` and locates:
   - `prospectus_url` — 424B4 or 424B3 filed within ±3/21 days of EFFECT date
   - `ipo_8k_url` — earliest 8-K with items 1.01 + 3.02 after EFFECT date
   - `tenk_urls` — all 10-K filings after EFFECT date, sorted ascending

2. **`extract_from_424b4(url)`** — Claude Haiku reads the prospectus and extracts:
   `offer_price`, `securities_offered`, `overallotment_option`, `auditor`, `audit_report_date`, `underwriters_list`

3. **`extract_from_8k(url)`** — Claude Haiku reads the IPO consummation 8-K (Items 1.01 / 3.02) and extracts:
   `ipo_date`, `ticker*`, `exchange`, `overallotment_exercised`, `overallotment_exercised_date`, `pp_securities*`

Claude model used throughout: **`claude-haiku-4-5-20251001`** with prompt caching (`cache_control: ephemeral` on system prompt).

---

## 10-K Verification (`extract_from_10k`)

Used in the **IPO Verification** admin tab to cross-check data already in the DB against the first 10-K filing.

### Ticker extraction (priority order)
1. **iXBRL tags** (`_parse_tickers_from_xbrl`) — parses `dei:TradingSymbol` elements, resolves each `contextRef` to an `xbrli:context`, reads `StatementClassOfStockAxis` member to classify as common/units/warrants/rights. Also reads `dei:SecurityExchangeName` for the exchange. Most reliable for modern iXBRL filings.
2. **Section 12(b) regex** (`_parse_tickers_from_sec12b`) — fallback for older filings lacking iXBRL. Parses pipe-separated HTML table output or stripped text between "Section 12(b)" and "Section 12(g)".
3. **Claude** — last resort; still asked in the prompt but overridden by either method above.

### Other fields
Claude extracts: `ipo_date`, `offer_price`, `securities_offered`, `overallotment_exercised`, `overallotment_exercised_date`, `pp_securities*`.

**Post-processing:** if `overallotment_exercised` is set but `overallotment_exercised_date` is null and `ipo_date` is known, `overallotment_exercised_date` defaults to `ipo_date` (handles "simultaneously with closing" language).

### Anchor selection
Finds the earliest occurrence (> 2,000 chars into the document) of phrases like "consummated our initial public offering" to skip the cover page / table of contents and land in the Note 1 narrative.

---

## EDGAR EFFECT scraper (`edgar_scraper.py`)

Queries `https://efts.sec.gov/LATEST/search-index` with `forms=EFFECT` for a date range, paginates in batches of 100. For each hit, calls the EDGAR submissions API to get company metadata. Filters to **SIC 6770** (blank check / SPAC) only. Sends an HTML email; skips the email if no SPACs filed that day.

**GitHub Actions trigger:** 1st of each month at 9:30 AM ET, covering the entire previous month. Can also be dispatched manually with a single date or a date range.

**Required GitHub secrets:** `EDGAR_USER_AGENT`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `EMAIL_FROM`, `EMAIL_TO`

---

## PCAOB audit partner lookup

`load_pcaob_form_ap()` — downloads the PCAOB Form AP ZIP from `pcaobus.org`, parses the CSV. `lookup_audit_partner(cik, audit_report_date)` matches by CIK + audit report date to find the engagement partner ID, which links to the `pcaob_partners` table.

---

## Key decisions / gotchas

- **`verified` column** must be added manually in Supabase (`boolean`, default `false`). The app will error without it.
- **`service_role_key`** must never be cached with `@st.cache_resource` — it's instantiated fresh for each write to avoid stale connections.
- **Anchor matching** requires position > 2,000 chars to avoid matching the table of contents or cover page footnotes.
- **OA base count:** if the 10-K says "X Units, including Y Units for over-allotment," then `securities_offered = X - Y`. Claude is explicitly instructed on this.
- **Founder shares** must not be confused with private placement — the prompt explicitly excludes shares issued for nominal consideration (e.g. $25,000 for millions of shares).
- **iXBRL context axis members** vary by filer (e.g. `us-gaap:CommonClassAMember`, `UnitsMember`, `RedeemableWarrantsMember`) — the classifier uses keyword matching on the lowercased member name after stripping the namespace prefix.
