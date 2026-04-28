# SPAC IPO Tracker — Complete Workflow Documentation

## What Is This System?

This is a research tool that tracks **Special Purpose Acquisition Companies (SPACs)** that have completed their IPOs. A SPAC is a "blank check company" — it raises money from public investors with no specific acquisition target in mind yet. Later, it merges with a private company to take it public.

The system does three things:
1. **Discovers** new SPAC IPOs automatically by watching SEC EDGAR every day
2. **Extracts** financial details from the SEC filings (prospectus, 8-K, and annual 10-K report) using AI
3. **Presents** a clean, searchable database to researchers through a web app

**Who uses it:** Vincent Castellani (admin) manages the data. Researchers browse the public view.

---

## Technology Stack

| Component | What It Is | Role |
|-----------|-----------|------|
| **Streamlit** | Python web framework | Powers the entire web app (both public and admin views) |
| **Supabase** | Postgres database hosted in the cloud | Stores all SPAC data |
| **SEC EDGAR** | SEC's public filing database | Source of all IPO data |
| **Claude AI (Haiku)** | Anthropic's AI model | Reads SEC documents and extracts structured data |
| **GitHub Actions** | Automated job runner | Runs the EDGAR scraper on a schedule |
| **PCAOB** | Public Company Accounting Oversight Board | Source of audit partner data |

---

## The Database (Supabase)

There are three tables:

### `ipos` — The main table
One row per confirmed SPAC IPO. This is what the public sees (filtered to verified records only).

Key columns:

| Column | Example | What It Means |
|--------|---------|---------------|
| `company_name` | "Churchill Capital Corp" | The SPAC's name |
| `cik` | "0001776197" | SEC's unique company identifier |
| `ipo_date` | 2020-09-11 | Date the IPO actually closed |
| `size_m` | 690.0 | IPO size in millions of dollars |
| `prospectus_url` | https://... | Link to the 424B4 prospectus on SEC EDGAR |
| `verified` | true/false | Has Vincent manually verified this record? Only verified records are public |
| `ticker` | "CCIX" | Common stock ticker symbol |
| `ticker_units` | "CCIXU" | Units ticker (stock + warrant bundled together) |
| `ticker_warrants` | "CCIXW" | Warrant ticker |
| `ticker_rights` | null | Rights ticker (some SPACs issue rights instead of warrants) |
| `exchange` | "NYSE" | Which stock exchange it listed on |
| `securities_offered` | 69000000 | Number of units sold in the base IPO (NOT including over-allotment) |
| `offer_price` | 10.00 | Price per unit (almost always $10.00 for SPACs) |
| `overallotment_option` | 9000000 | Max additional units underwriters could buy |
| `overallotment_exercised` | 9000000 | Actual additional units bought |
| `overallotment_exercised_date` | 2020-09-14 | Date underwriters exercised that option |
| `pp_securities` | 10800000 | Private placement warrants/units sold to the sponsor simultaneously |
| `pp_securities_type` | "Warrants" | What type of security the sponsor bought |
| `pp_price` | 1.50 | Price per private placement security |
| `auditor` | "WithumSmith+Brown" | Audit firm |
| `audit_report_date` | "September 14, 2020" | Date the auditors signed the audit report |
| `audit_partner_id` | "P12345" | PCAOB engagement partner ID |
| `underwriters_list` | ["Citigroup", "Goldman Sachs"] | Lead underwriters |
| `filings` | [{type: "424B4", url: ...}] | JSON array of all associated SEC filing URLs |

### `pending_ipos` — The review queue
Newly discovered SPACs land here first, before Vincent reviews them.

| Column | What It Means |
|--------|---------------|
| `company_name` | SPAC name from EDGAR |
| `cik` | SEC CIK number |
| `effect_date` | Date the registration became effective on EDGAR |
| `status` | Always "pending" until rejected (rejected records are deleted) |
| `created_at` | When the scraper found it |

### `pcaob_partners` — Audit partners
Engagement partner records downloaded from the PCAOB, linked to `ipos` by `audit_partner_id`.

---

## The Full Workflow — Step by Step

---

### PART 1: Automated Daily Discovery

**File:** `edgar_scraper.py`
**Trigger:** GitHub Actions, runs on the 1st of each month at 9:30 AM Eastern (and can be triggered manually for any date range)

#### What happens:

**Step 1 — Query EDGAR for EFFECT filings**

The SEC has a filing type called "EFFECT" — it means a company's registration statement just became effective, i.e., they are now legally allowed to sell securities to the public. The scraper queries the SEC's full-text search API:

```
https://efts.sec.gov/LATEST/search-index?forms=EFFECT&startdt=YYYY-MM-DD&enddt=YYYY-MM-DD
```

It pages through results in batches of 100 until all filings for the day are collected.

**Step 2 — Filter to SPACs only (SIC code 6770)**

For each filing, the scraper calls the SEC submissions API to get the company's SIC (Standard Industrial Classification) code:

```
https://data.sec.gov/submissions/CIK0001776197.json
```

Only **SIC code 6770** (Blank Check Companies) are SPACs. All others are ignored.

**Step 3 — Send email digest**

An HTML email is sent listing each new SPAC EFFECT filing with the company name, CIK, SIC code, accession number, and a link to the EDGAR profile and PCAOB auditor search. If no SPACs filed that day, the email is skipped.

**Step 4 — Push to pending queue**

Simultaneously, each SPAC's `company_name`, `cik`, and `effect_date` is posted to the `pending_ipos` Supabase table via the REST API. A `UNIQUE(cik)` constraint means running the scraper twice for the same date is safe — duplicates are silently ignored.

---

### PART 2: The Pending Queue (Admin Tab)

Once the scraper runs, Vincent opens the app and goes to **Admin Panel → Pending Queue**.

He sees a list of all newly discovered SPACs waiting for review. For each one:

**Option A — Extract & Review:**
1. Click **"Extract from EDGAR & Load into Add Form"**
2. The app automatically:
   - Calls `find_edgar_urls(cik, effect_date)` to locate the 424B4 prospectus, the IPO 8-K, and any 10-K filings in the company's EDGAR history
   - Calls `extract_from_424b4(url)` — AI reads the prospectus (see Part 3)
   - Calls `extract_from_8k(url)` — AI reads the IPO 8-K (see Part 3)
3. The Add New Entry form is pre-filled with all extracted data
4. Switch to the **Add New Entry** tab, review/correct any fields, and click Submit
5. The record is inserted into `ipos` and automatically removed from `pending_ipos`

**Option B — Reject:**
Click **Reject** to delete the record from `pending_ipos` without adding it to the database (e.g., if it's not actually a SPAC IPO or is a duplicate).

---

### PART 3: Data Extraction from SEC Filings

Three different SEC documents are read using Claude AI (Haiku model). Each document is fetched, HTML tags are stripped, and the cleaned text is sent to Claude with a structured prompt. Claude returns a JSON object.

#### 3A — The 424B4 Prospectus

The 424B4 is the final prospectus filed when the IPO actually closes. It contains the offering terms.

**How it's found:** The filing dated within 3 days before or 21 days after the EFFECT date, form type "424B4" or "424B3".

**What Claude extracts:**
- `offer_price` — price per unit
- `securities_offered` — base IPO unit count
- `overallotment_option` — size of the overallotment option
- `auditor` — name of the audit firm
- `audit_report_date` — date the auditors signed (format: "Month DD, YYYY")
- `underwriters_list` — list of underwriter names

#### 3B — The IPO 8-K

When a SPAC closes its IPO, it files an 8-K (current report) with Items 1.01 (Entry into Material Agreement) and 3.02 (Unregistered Sales of Equity Securities). This is the definitive record of what actually happened on closing day.

**How it's found:** The earliest 8-K after the EFFECT date with both items 1.01 and 3.02.

**What Claude extracts:**
- `ipo_date` — the actual closing date
- `ticker`, `ticker_units`, `ticker_warrants`, `ticker_rights` — all trading symbols
- `exchange` — NYSE, NASDAQ, or AMEX
- `overallotment_exercised` — how many over-allotment units were actually sold
- `overallotment_exercised_date` — when (if different from IPO date)
- `pp_securities`, `pp_securities_type`, `pp_price` — the private placement details (what the sponsor bought simultaneously)
- Second private placement tranche if one exists

#### 3C — The 10-K Annual Report (Verification Only)

The 10-K is the annual report filed roughly one year after the IPO. The first 10-K contains a "Note 1 — Organization and Business Operations" section that summarizes the IPO terms in retrospect. This is used during the **verification stage** (Part 5) to cross-check the data already in the database.

**Ticker extraction from 10-K (priority order):**

1. **iXBRL tags (primary):** Modern 10-K filings use Inline XBRL markup. The `dei:TradingSymbol` element is tagged with a `contextRef` that links to an `xbrli:context` containing a `StatementClassOfStockAxis` dimension. This dimension tells us exactly what class of security the ticker belongs to (e.g., `CommonClassAMember` → common stock ticker, `UnitsMember` → units ticker, `WarrantMember` → warrants). This is parsed programmatically — no AI needed.

2. **Section 12(b) table (fallback):** Older filings without iXBRL have a cover-page table listing each registered security's description, ticker, and exchange. This is extracted by finding the "Section 12(b)" HTML block and parsing it.

3. **Claude (last resort):** If neither structured method finds tickers, Claude reads the text.

**Over-allotment date fallback:** If Claude extracts an over-allotment exercise quantity but returns null for the date, and the text described it as happening "simultaneously" or "concurrently" with the IPO closing, the code automatically sets the date equal to the IPO date.

---

### PART 4: The Public Web App

The public-facing app (no login required) shows only **verified** records (`verified = true` in the database).

**Main table columns:** Company Name | CIK | IPO Date | Size ($M) | Prospectus | Verified ✓

**Analytics chart:** Bar chart of IPOs per year.

**Detail view:** Click any row to expand a card with all fields — pricing, securities, private placement, over-allotment, tickers, auditor, underwriters, and all filing links.

---

### PART 5: Admin Panel (Password-Protected)

Accessible via the sidebar password. Four tabs:

#### Tab 1 — Add New Entry
Manual entry form with EDGAR pre-fill. Enter a CIK and EFFECT date, click "Find & Extract," and the form auto-populates from EDGAR. Review, edit if needed, and submit.

#### Tab 2 — Edit / Delete
Select any existing record by name, edit any field, and save. Or delete the record entirely. Also lets Vincent manually add/edit 10-K filing URLs.

#### Tab 3 — IPO Verification
For records that have a 10-K filing URL but haven't been verified yet. Select a company, click "Extract from 1st 10-K," and the app reads the 10-K and displays a side-by-side comparison table:

| Field | Stored in DB | Extracted from 10-K |
|-------|-------------|---------------------|
| IPO Date | 2020-09-11 | 2020-09-11 |
| Securities Offered | 69,000,000 | 69,000,000 |
| OA Exercised | 9,000,000 | 9,000,000 |
| OA Exercised Date | 2020-09-14 | 2020-09-14 |
| Ticker | CCIX | CCIX |
| ... | | |

If everything matches, click **"Mark as Verified"** — this sets `verified = true`, making the record visible to the public.

#### Tab 4 — Pending Queue
As described in Part 2 above.

---

### PART 6: PCAOB Audit Partner Lookup

When a new entry is added, if an `audit_report_date` was extracted, the system automatically tries to identify the engagement partner:

1. Downloads the PCAOB Form AP ZIP file from `pcaobus.org` (cached for 24 hours)
2. Parses the CSV to find rows matching the company's CIK and audit report date
3. If found, stores the `engagement_partner_id` in the `ipos` table
4. This links to the `pcaob_partners` table, which is populated separately via a button in the admin panel

---

## Setup Requirements

### Streamlit Secrets (`secrets.toml`)
```toml
[supabase]
url              = "https://your-project.supabase.co"
anon_key         = "eyJ..."
service_role_key = "eyJ..."

anthropic_api_key = "sk-ant-..."
admin_password    = "your-password"
```

### GitHub Secrets (for the Actions workflow)
| Secret | What It Is |
|--------|-----------|
| `EDGAR_USER_AGENT` | Contact string for EDGAR API (e.g., "IPOTracker/1.0 you@example.com") |
| `SMTP_HOST` | Email server hostname |
| `SMTP_PORT` | Email server port (usually 587) |
| `SMTP_USERNAME` | Email login |
| `SMTP_PASSWORD` | Email password |
| `EMAIL_FROM` | Sender address |
| `EMAIL_TO` | Recipient address(es), comma-separated |
| `SUPABASE_URL` | Same URL as in Streamlit secrets |
| `SUPABASE_SERVICE_KEY` | Same service role key as in Streamlit secrets |

### Supabase — One-Time Setup
The `verified` column and `pending_ipos` table must be created manually:

```sql
-- Add verified column to ipos if it doesn't exist
ALTER TABLE ipos ADD COLUMN IF NOT EXISTS verified BOOLEAN DEFAULT false;

-- Create pending queue table
CREATE TABLE pending_ipos (
  id           BIGSERIAL PRIMARY KEY,
  company_name TEXT NOT NULL,
  cik          TEXT NOT NULL,
  effect_date  DATE NOT NULL,
  status       TEXT NOT NULL DEFAULT 'pending',
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(cik)
);
```

---

## Common Gotchas

| Issue | Why It Happens | Fix |
|-------|---------------|-----|
| Record doesn't appear publicly | `verified` is still false | Go to IPO Verification tab and mark it verified |
| Extraction returns wrong securities count | 10-K says "X units including Y for over-allotment" — base is X minus Y | The AI prompt handles this, but double-check the math |
| OA date is same as IPO date | Over-allotment was exercised simultaneously on closing day | Correct — the code defaults it to IPO date when text says "simultaneously" |
| Tickers missing from 10-K extraction | Filing is older HTML without iXBRL tags | Section 12(b) regex fallback runs automatically |
| Pending queue push silently skipped | `SUPABASE_URL` / `SUPABASE_SERVICE_KEY` not set in GitHub secrets | Add both secrets to the GitHub repo |
| Duplicate pending entry blocked | Same CIK already in `pending_ipos` | Expected behavior — `UNIQUE(cik)` prevents re-inserting the same SPAC |
| 424B4 not found | Filing was more than 21 days after the EFFECT date | Manually enter the prospectus URL in the Add form |
| Auditor partner ID not found | PCAOB Form AP hasn't been filed yet, or date doesn't match exactly | Leave blank; update later via Edit tab |

---

## File Map

```
IPOs/
├── app.py                              # Entire Streamlit web app
├── edgar_scraper.py                    # Daily EDGAR scraper + email + pending push
├── CLAUDE.md                           # Technical reference for the AI assistant
├── WORKFLOW.md                         # This document
├── requirements.txt                    # Python dependencies
└── .github/
    └── workflows/
        └── monthly_edgar_pull.yml      # GitHub Actions job definition
```

---

## Data Flow Summary

```
SEC EDGAR (daily EFFECT filings)
        │
        ▼
edgar_scraper.py  ──► Email to Vincent (SPAC digest)
        │
        ▼
pending_ipos table (Supabase)
        │
        ▼
Admin: Pending Queue tab
        │
        ├── Extract from EDGAR ──► 424B4 (Claude) ──► offering terms
        │                     └──► 8-K (Claude)   ──► IPO date, tickers, PP details
        │
        ▼
Admin: Add New Entry tab (review & submit)
        │
        ▼
ipos table (verified = false)
        │
        ▼
Admin: IPO Verification tab ──► 10-K (iXBRL + Claude) ──► cross-check
        │
        ▼
ipos table (verified = true)
        │
        ▼
Public web app (researchers can see it)
```
