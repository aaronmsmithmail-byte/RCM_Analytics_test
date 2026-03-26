# Healthcare RCM Analytics Dashboard

A comprehensive Streamlit web application for monitoring and analyzing Healthcare Revenue Cycle Management (RCM) KPIs and metrics, built on a production-grade medallion data architecture.

## Overview

This dashboard provides healthcare organizations with interactive visualizations across twelve analytical tabs plus five metadata pages:

| Tab | Description |
|-----|-------------|
| **Executive Summary** | High-level KPI scorecard with configurable alert thresholds |
| **Collections & Revenue** | Revenue waterfall, collection rate trends, cost to collect |
| **Claims & Denials** | Denial reasons, clean claim rate, first-pass resolution, charge lag, scrubbing rule breakdown |
| **A/R Aging & Cash Flow** | Aging buckets, days in A/R trend, monthly cash flow |
| **Payer Analysis** | Revenue/volume/denial rate breakdown by payer with claim-level drill-down |
| **Department Performance** | Revenue, collection rate, and encounter volume by department with encounter-level drill-down |
| **Provider Performance** | Provider scorecard with collection rate, denial rate, and clean claim rate outlier detection |
| **CPT Code Analysis** | Revenue, denial rate, and charge concentration by procedure code |
| **Underpayment Analysis** | ERA allowed vs. paid variance — contractual recovery opportunity by payer |
| **📈 Forecasting** | Linear trend projections for cash flow, DAR, and denial rate + interactive what-if scenario modelling |
| **Patient Responsibility** | Patient-owed portion (co-pay/deductible/coinsurance) by payer, department, and encounter type |
| **AI Assistant** | Natural-language chat interface — asks questions, queries the database live via tool calling, and explains results in plain language |

**Metadata pages** (sidebar navigation): Data Catalog · Data Lineage · Knowledge Graph · Semantic Layer · AI Architecture

Every data tab includes **CSV and Excel export buttons**. A **KPI alert system** in the sidebar flags threshold breaches in real time, and a **data pipeline freshness panel** shows the last ETL run time and status for each of the 10 data domains.

The **AI Assistant** tab uses an agentic tool-calling loop: the model can call `run_sql()` to execute live SELECT queries against the SQLite database, receive the results, and weave them into its answer — all within a single conversational turn.  Requires a free [OpenRouter](https://openrouter.ai/keys) API key.

---

## Requirements

- Python 3.9+
- pip

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd RCM_Analytics_test
```

### 2. Create and activate a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate        # macOS / Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Generate sample data

```bash
python generate_sample_data.py
```

This creates 10 CSV files in the `data/` directory covering Jan 2024 – Dec 2025:

| File | Rows | Source System | Description |
|------|------|---------------|-------------|
| `payers.csv` | 10 | Payer Master | Commercial, government, and self-pay payers |
| `patients.csv` | 500 | EHR | Patient demographics and insurance info |
| `providers.csv` | 25 | EHR | Providers across 10 departments |
| `encounters.csv` | 3,000 | EHR | Patient encounters (outpatient, inpatient, ED, telehealth) |
| `charges.csv` | ~5,900 | EHR / Charge Capture | Charge records with CPT and ICD-10 codes |
| `claims.csv` | 2,800 | Clearinghouse | Claims with status and scrubbing fail reason |
| `payments.csv` | ~3,200 | Clearinghouse / ERA | Payments with allowed amount and accuracy flags |
| `denials.csv` | ~400 | Clearinghouse / ERA | Denial records with appeal tracking |
| `adjustments.csv` | 600 | Billing System | Contractual, writeoff, and charity adjustments |
| `operating_costs.csv` | 24 | ERP / Finance | Monthly RCM operational costs |

On first launch, the app automatically loads these CSVs into a local SQLite database using the medallion pipeline (Bronze → Silver → Gold). No manual database setup is required.

> **Schema migration:** If you regenerate sample data after a previous run, the app detects the schema version automatically and rebuilds all three medallion layers cleanly.

### 5. (Optional) Create a local configuration file

Copy the provided example to create your own `.env`:

```bash
cp .env.example .env
```

Then open `.env` and fill in any values you want to override.  At minimum, set `OPENROUTER_API_KEY` to enable the AI Assistant tab — all other variables have sensible defaults and can be left commented out.

```
OPENROUTER_API_KEY=your_key_here   # get a free key at openrouter.ai/keys
```

The `.env` file is listed in `.gitignore` and will never be committed.  See the [Configuration Reference](#configuration-reference) section below for all available variables.

### 6. Run the dashboard

```bash
streamlit run app.py
```

The app opens at `http://localhost:8501`. The first launch initializes the database (a few seconds); subsequent launches use the cached database.

---

## Data Architecture — Medallion Layers

All data flows through a three-layer medallion architecture stored in a local SQLite database (`rcm_analytics.db`):

| Layer | Tables/Views | Description |
|-------|-------------|-------------|
| **Bronze** | 10 tables (`bronze_*`) | Raw TEXT ingestion from CSV — no type casting, full audit trail via `_loaded_at` timestamp |
| **Silver** | 10 tables (`silver_*`) | Cleaned, typed (REAL/INTEGER/TEXT), FK-constrained — source of truth for all KPI computation |
| **Gold** | 5 views (`gold_*`) | Pre-aggregated business views computed at query time from Silver |

A `pipeline_runs` metadata table records the last load time, row count, and source file for each domain. The dashboard reads this table to power the data freshness sidebar panel.

### Gold Views

| View | Description |
|------|-------------|
| `gold_monthly_kpis` | Monthly claim counts, charges, payments, CCR, denial rate, GCR |
| `gold_payer_performance` | Revenue, volume, and collection metrics per payer |
| `gold_department_performance` | Revenue, encounter count, and revenue-per-encounter by department |
| `gold_ar_aging` | Outstanding AR grouped into 0–30, 31–60, 61–90, 91–120, 120+ day buckets |
| `gold_denial_analysis` | Denial volume, dollars denied, and recovery rate by reason code |

### Filtering with FilterParams

All metric functions accept a `FilterParams` dataclass that applies four filter dimensions at the Silver layer via parameterized SQL:

```python
FilterParams(
    start_date="2024-01-01",   # inclusive
    end_date="2024-12-31",     # inclusive
    payer_id="PYR001",         # optional — None for all payers
    department="Cardiology",   # optional — None for all departments
    encounter_type="Inpatient" # optional — None for all encounter types
)
```

A shared `WITH filtered_claims AS (...)` CTE joins `silver_claims` to `silver_encounters` and applies all active filter conditions, ensuring consistent filtering across every metric.

---

## Project Structure

```
RCM_Analytics_test/
├── app.py                   # Main Streamlit dashboard (12 tabs + sidebar)
├── generate_sample_data.py  # Synthetic data generation script
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container build for deployment
├── .env                     # OPENROUTER_API_KEY (not committed — create locally)
├── .streamlit/
│   └── config.toml          # Streamlit server and theme settings
├── .github/
│   └── workflows/
│       └── test.yml         # CI pipeline (runs pytest on every push/PR)
├── data/                    # CSV data files + rcm_analytics.db (generated)
├── src/
│   ├── __init__.py
│   ├── database.py          # Medallion schema, ETL, build_filter_cte(), schema migration
│   ├── data_loader.py       # Sidebar widget population helpers
│   ├── metadata_pages.py    # Data Catalog, Data Lineage, Knowledge Graph, Semantic Layer,
│   │                        #   AI Architecture (process flow diagram)
│   ├── metrics.py           # SQL-based KPI engine (23 query_* functions + FilterParams)
│   ├── ai_chat.py           # AI Assistant backend: TOOL_SCHEMA, execute_sql_tool(),
│   │                        #   run_agentic_turn(), build_system_prompt()
│   └── validators.py        # SQL COUNT-based data integrity checks
└── tests/
    ├── __init__.py
    ├── test_metrics.py      # 110 unit tests for KPI metric functions
    └── test_validators.py   # 40 unit tests for data validators
```

---

## Configuration Reference

All configuration is handled through environment variables loaded from a `.env` file.  Copy `.env.example` to `.env` and override only what you need — every variable has a default.

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `OPENROUTER_API_KEY` | — | Yes (AI tab only) | API key for the OpenRouter LLM gateway. Get a free key at [openrouter.ai/keys](https://openrouter.ai/keys). All other tabs work without it. |
| `OPENROUTER_MODEL` | `openai/gpt-4o-mini` | No | LLM model for the AI Assistant tab. Any model on [openrouter.ai/models](https://openrouter.ai/models) works. |
| `AI_MAX_ROWS` | `100` | No | Maximum rows the AI tool returns per SQL query. Lower to reduce token cost; raise for wider result sets (min 10). |
| `AI_MAX_ITERATIONS` | `8` | No | Maximum tool-call loop iterations per AI turn — one iteration = one SQL query + one LLM round-trip (min 1). |
| `RCM_DB_PATH` | `./data/rcm_analytics.db` | No | Path to the SQLite database file. Override for Docker volume mounts or shared network paths. |
| `RCM_DATA_DIR` | `./data/` | No | Directory containing the CSV source files. Used by `generate_sample_data.py` and the ETL pipeline. |
| `STREAMLIT_SERVER_PORT` | `8501` | No | Port the Streamlit server listens on. Standard Streamlit env var. |
| `STREAMLIT_SERVER_ADDRESS` | `localhost` | No | Bind address. Set to `0.0.0.0` to accept external connections (Docker). |
| `STREAMLIT_BROWSER_GATHER_USAGE_STATS` | `true` | No | Set to `false` to disable Streamlit's anonymous usage reporting in production. |

> **Adding a new variable?**  Add it to `.env.example` with a comment explaining the default, expected values, and impact.  Then update this table.

---

## Metrics Reference

### Core KPIs (pre-computed for the alert system)

| # | Metric | Formula | Benchmark |
|---|--------|---------|-----------|
| 1 | Days in A/R (DAR) | AR Balance / Avg Daily Charges | ≤ 35 days |
| 2 | Net Collection Rate (NCR) | Payments / (Charges − Contractual Adj) | ≥ 95% |
| 3 | Gross Collection Rate (GCR) | Total Payments / Total Charges | ≥ 70% |
| 4 | Clean Claim Rate (CCR) | Clean Claims / Total Claims | ≥ 90% |
| 5 | Denial Rate | Denied Claims / Total Claims | ≤ 10% |
| 6 | First-Pass Resolution Rate | Paid on First Submission / Total Claims | ≥ 85% |
| 7 | Payment Accuracy Rate | Accurate Payments / Total Payments | ≥ 95% |
| 8 | Bad Debt Rate | Bad Debt Write-offs / Total Charges | ≤ 3% |

### Additional Metrics

| # | Metric | Description |
|---|--------|-------------|
| 9 | Denial Reasons Breakdown | Grouped by reason code with recovery rate |
| 10 | Charge Lag | Avg days from service date to charge post date |
| 11 | Cost to Collect | Total RCM Costs / Total Collections |
| 12 | A/R Aging Buckets | Outstanding AR by 0–30, 31–60, 61–90, 91–120, 120+ days |
| 13 | Appeal Success Rate | Won Appeals / Total Appealed |
| 14 | Avg Reimbursement per Encounter | Total Payments / Total Encounters |
| 15 | Payer Mix Analysis | Revenue and volume by payer |
| 16 | Denial Rate by Payer | Denials per payer / Total payer claims |
| 17 | Department Performance | Revenue, collection rate, encounters by department |
| 18 | Provider Performance | Collection rate, denial rate, CCR, avg payment per provider |
| 19 | CPT Code Analysis | Revenue, denial rate, avg charge per unit by procedure code |
| 20 | Underpayment Analysis | ERA allowed_amount vs. payment_amount variance by payer |
| 21 | Clean Claim Scrubbing Breakdown | Dirty claim root causes by fail reason code |
| 22 | Patient Financial Responsibility | Patient-owed portion (co-pay/deductible) by payer, dept, trend |
| 23 | Data Freshness | Last ETL load time, row count, and staleness per domain |

All metrics are implemented as parameterized SQL queries against the Silver layer (`query_*` functions in `src/metrics.py`).

---

## Dashboard Tabs

### Tab 1 — Executive Summary
A single-screen scorecard for leadership. Eight color-coded KPI cards (green/amber/red vs. benchmarks) cover DAR, NCR, CCR, Denial Rate, GCR, First-Pass Rate, Payment Accuracy, and Bad Debt Rate. Trend lines for DAR and NCR give a month-over-month view; a grouped volume bar chart shows monthly encounters and claims.

**Alert banner:** If any KPI breaches its configured threshold, a banner appears at the top of this tab listing each breach with current value vs. threshold.

### Tab 2 — Collections & Revenue
Revenue waterfall from gross charges → contractual adjustments → denials → net collections. Supporting charts: gross vs. net collection rate trends, cost-to-collect trend vs. the 5% target, average reimbursement per claim. Financial summary table with CSV/Excel export.

### Tab 3 — Claims & Denials
Claim status distribution, top denial reasons by volume and dollar amount, denial/CCR/first-pass trend lines, and charge lag histogram.

**Claim Scrubbing Rules Breakdown:** A dedicated section below the denial analysis surfaces *why* dirty claims fail. Each dirty claim is tagged with one of six clearinghouse edit codes:

| Code | Description | Resolution Guidance |
|------|-------------|---------------------|
| `MISSING_AUTH` | Missing Prior Authorization | Automate PA check at scheduling |
| `ELIGIBILITY_FAIL` | Eligibility Not Verified | Real-time eligibility check 24-48h before visit |
| `CODING_ERROR` | Invalid CPT/ICD-10 Combination | Add coding edit rules; schedule coder training |
| `DUPLICATE_SUBMISSION` | Duplicate Claim | Enable duplicate detection in clearinghouse |
| `TIMELY_FILING` | Outside Filing Window | Automate timely-filing deadline alerts |
| `MISSING_INFO` | Missing Required Information | Front-desk registration checklist with required fields |

Charts show dirty claim volume and charges-at-risk by fail reason. An expandable table includes resolution guidance and is exportable.

### Tab 4 — A/R Aging & Cash Flow
Aging bucket bar/pie charts, dual-axis A/R balance vs. Days in A/R trend, monthly cash flow (charges vs. payments + net cash flow line).

### Tab 5 — Payer Analysis
Revenue and claim volume by payer, ranked collection rate and denial rate bars, payer comparison table. **Payer drill-down:** claim count, charges, payments, denied claims for any selected payer + claim status pie, denial reasons bar, and claim-level table with export.

### Tab 6 — Department Performance
Charges vs. payments by department, collection rate ranking, encounter volume pie, avg payment per encounter, encounter type breakdown. **Department drill-down:** encounter count, claim count, charges, payments for any selected department + encounter type pie, claim status bar, and encounter/claim table with export.

### Tab 7 — Provider Performance
Individual provider scorecard with collection rate, denial rate, and clean claim rate. Outlier detection flags providers with denial rate > 15%. Charts: revenue by provider, denial rate ranking, CCR ranking, avg payment per encounter. **Provider drill-down:** peer comparison deltas (vs. average), claim status mix, and top denial reasons.

### Tab 8 — CPT Code Analysis
Revenue and denial patterns by procedure code. Top CPT codes by revenue (colored by denial rate), highest-denial CPT codes (min 10 charges threshold), revenue concentration pie for top 12 codes, avg charge per unit. Full CPT detail table with export.

### Tab 9 — Underpayment Analysis
Compares ERA `allowed_amount` (contracted rate) vs. `payment_amount` (actual remittance). Identifies systematic payer underpayments — a direct contractual recovery opportunity. Summary KPIs: total recovery opportunity, underpayment rate, underpaid claim count, avg shortfall per claim. Charts: recovery opportunity by payer, underpayment rate by payer, monthly trend, allowed/paid/underpaid stacked waterfall. Payer summary table with export.

### Tab 10 — 📈 Forecasting
**Trend projections:** Linear extrapolation (degree-1 polynomial) with ±1 std-dev confidence band projected 3 months forward for cash flow (monthly collections), Days in A/R, and denial rate. Each projection includes a pass/fail callout against its benchmark.

**What-If Scenario Modelling** — three interactive sliders:

| Scenario | Output |
|----------|--------|
| Reduce denial rate by X pp | Monthly/annual claim recovery ($) + claims recovered/month |
| Reduce DAR by X days | One-time cash acceleration ($) + new projected DAR |
| Improve clean claim rate by X pp | Monthly/annual rework savings ($) + implied denial rate drop |

A **Combined Annual Impact** summary row aggregates all three scenarios.

### Tab 11 — Patient Responsibility
Analyses the patient-owed portion of the revenue cycle — co-pays, deductibles, and coinsurance — derived from `allowed_amount − payment_amount` in ERA payment data (no additional data needed).

Summary KPIs: total patient responsibility, patient responsibility rate (% of allowed), avg per claim, self-pay exposure. Charts: total patient responsibility by payer (colored by % of allowed), by payer type with rate labels, monthly trend, by department and encounter type. Payer detail table with export.

### Tab 12 — AI Assistant

A conversational interface backed by an **agentic tool-calling loop**:

1. A system prompt is built fresh each turn from the four `meta_*` tables (KPI definitions, semantic mappings, entity descriptions, relationships) plus the current live KPI values and active sidebar filters.
2. The selected model (via OpenRouter) reasons over the context and decides whether to answer from the snapshot or call `run_sql()`.
3. `run_sql()` executes a read-only SELECT/WITH query against the SQLite database, returns structured results (capped at 100 rows), and feeds them back to the model.  The loop repeats until the model returns a final text response.
4. Each SQL query issued by the model is shown in a collapsible expander with the exact SQL and a scrollable results table.

**Setup:** add `OPENROUTER_API_KEY=<your_key>` to a `.env` file in the project root and restart the app.  Get a free key at [openrouter.ai/keys](https://openrouter.ai/keys).

### Metadata Pages (sidebar navigation)

- **Data Catalog** — Searchable reference of all 23 KPIs and 10 data tables with descriptions, formulas, and source columns
- **Data Lineage** — DAG diagram showing the full pipeline: CSV files → Bronze → Silver → Gold → Dashboard
- **Knowledge Graph** — Entity-relationship diagram of the Silver-layer data model
- **Semantic Layer** — Business concept → KPI → source table/column mapping for every metric
- **AI Architecture** — Interactive process flow diagram showing how the AI chat tab assembles context from the semantic layer and knowledge graph, routes through the LLM, and executes live SQL queries to answer questions

---

## Sidebar Features

### Filters
All visualizations respond in real time to:
- **Date Range** — Filter by date of service
- **Payer** — Filter to a specific payer or view all
- **Department** — Filter to a specific clinical department
- **Encounter Type** — Outpatient, Inpatient, Emergency, Telehealth

Filters are applied at the database level via parameterized SQL — the Silver layer is queried directly with each selection.

### Alert Thresholds
An **⚙️ Alert Thresholds** expander lets users configure custom KPI targets for their organization. Defaults match industry benchmarks. Thresholds persist for the browser session.

When any KPI breaches its threshold, the sidebar shows a red alert badge with the count of breaching KPIs, and the Executive Summary tab displays an alert banner with value vs. threshold for each one.

### Data Pipeline Freshness
A **🟢/🟡/🔴 Data Pipeline** panel shows the last ETL run time, row count, and staleness status for all 10 data domains. Status is computed against the expected refresh cadence per domain (e.g. claims: 4h, encounters: 2h, operating costs: 720h).

### Data Quality
On startup, `src/validators.py` runs six SQL COUNT assertions against the Silver tables:

| Check | Level |
|-------|-------|
| Negative monetary amounts | Warning |
| Orphaned foreign keys | Warning |
| Null values in required columns | Error |
| Dates outside the 2020–2030 range | Warning |
| Unexpected claim status values | Warning |
| Null values in boolean columns | Warning |

Issues appear in a collapsible **Data Quality** panel. Errors expand automatically; warnings are collapsed by default.

---

## Running with Docker

```bash
docker build -t rcm-analytics .
docker run -p 8501:8501 rcm-analytics
```

The app will be available at `http://localhost:8501`.

---

## CI / Continuous Integration

A GitHub Actions workflow (`.github/workflows/test.yml`) runs automatically on every push and pull request:

1. Checks out the code
2. Sets up Python 3.11 with pip caching
3. Installs dependencies
4. Generates sample data
5. Runs `pytest tests/ -v`

---

## Running Tests

```bash
pytest tests/ -v
```

**318 tests total** — 151 metric tests (`test_metrics.py`), 40 validator tests (`test_validators.py`), 22 ETL pipeline tests (`test_etl_pipeline.py`), 22 AI SQL tool tests (`test_ai_chat_sql.py`), 19 data loader tests (`test_data_loader.py`), 15 AI prompt tests (`test_ai_chat_prompt.py`), 15 database tests (`test_database.py`), 13 app utility tests (`test_app_utils.py`), 11 AI agentic loop tests (`test_ai_chat_agentic.py`), and 10 AI config tests (`test_ai_chat_config.py`). The metric, validator, and database suites use SQLite `tmp_path` fixtures that spin up an isolated in-memory database per test, insert representative Silver-layer rows, and assert on SQL query results. The ETL tests verify CSV→Bronze loading, Bronze→Silver type casting, boolean conversion, NULL/empty PK filtering, and duplicate handling. The data loader tests cover date parsing, boolean parsing, column validation, and full Silver/Gold integration loading. The app utility tests cover CSV/Excel export and linear trend forecasting. The AI prompt tests verify system prompt construction from meta_* tables and KPI snapshot injection. The AI agentic loop tests use mocked OpenAI clients to verify tool-calling flow, message history mutation, max iteration limits, and API key validation. The AI SQL tool tests verify read-only enforcement, result structure, NaN→None conversion, and the LLM result formatter. The AI config tests use `importlib.reload()` to verify env var parsing, bounds clamping, and non-numeric fallback behaviour for `AI_MAX_ROWS` and `AI_MAX_ITERATIONS`.

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| streamlit | ≥ 1.30.0 | Web framework and UI |
| pandas | ≥ 2.0.0 | Data manipulation and DataFrame results |
| plotly | ≥ 5.18.0 | Interactive visualizations |
| numpy | ≥ 1.24.0 | Numerical calculations and trend extrapolation |
| duckdb | ≥ 1.0.0 | Columnar OLAP database engine (Snowflake-like architecture) |
| openpyxl | ≥ 3.1.0 | Excel export support |
| openai | ≥ 1.0.0 | OpenRouter API client (OpenAI-compatible) for the AI tab |
| python-dotenv | ≥ 1.0.0 | Loads `OPENROUTER_API_KEY` from the `.env` file |
| streamlit-shadcn-ui | latest | Polished KPI metric cards |
| streamlit-extras | latest | Metric card styling |
| graphviz | ≥ 0.20.0 | Publication-quality diagrams for metadata pages (Data Lineage, Knowledge Graph, Semantic Layer, AI Architecture) |
| pytest | ≥ 7.0.0 | Unit testing (dev) |
