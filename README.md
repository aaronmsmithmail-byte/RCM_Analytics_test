# Healthcare RCM Analytics Dashboard

A comprehensive Streamlit web application for monitoring and analyzing Healthcare Revenue Cycle Management (RCM) KPIs and metrics.

## Overview

This dashboard provides healthcare organizations with interactive visualizations across six key domains:

- **Executive Summary** — High-level KPI scorecard with benchmarks
- **Collections & Revenue** — Revenue waterfall, collection rate trends, cost to collect
- **Claims & Denials** — Denial reasons, clean claim rate, first-pass resolution, charge lag
- **A/R Aging & Cash Flow** — Aging buckets, days in A/R trend, monthly cash flow
- **Payer Analysis** — Revenue/volume/denial rate breakdown by payer with claim-level drill-down
- **Department Performance** — Revenue, collection rate, and encounter volume by department with encounter-level drill-down

Every data tab includes **CSV and Excel export buttons** so you can pull the filtered data into your own tools. A **data quality panel** in the sidebar automatically flags any integrity issues (nulls, orphaned keys, unexpected values) on load.

## Requirements

- Python 3.9+
- pip

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

| File | Rows | Description |
|------|------|-------------|
| `payers.csv` | 10 | Commercial, government, and self-pay payers |
| `patients.csv` | 500 | Patient demographics and insurance info |
| `providers.csv` | 25 | Providers across 10 departments |
| `encounters.csv` | 3,000 | Patient encounters (outpatient, inpatient, ED, telehealth) |
| `charges.csv` | ~5,900 | Charge records with CPT and ICD-10 codes |
| `claims.csv` | 2,800 | Claims with status tracking |
| `payments.csv` | ~3,200 | Payments with accuracy flags |
| `denials.csv` | ~400 | Denial records with appeal tracking |
| `adjustments.csv` | 600 | Contractual, writeoff, and charity adjustments |
| `operating_costs.csv` | 24 | Monthly RCM operational costs |

### 5. Run the dashboard

```bash
streamlit run app.py
```

The app will open at `http://localhost:8501`.

## Project Structure

```
RCM_Analytics_test/
├── app.py                   # Main Streamlit dashboard
├── generate_sample_data.py  # Sample data generation script
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container build for deployment
├── .streamlit/
│   └── config.toml          # Streamlit server and theme settings
├── .github/
│   └── workflows/
│       └── test.yml         # CI pipeline (runs pytest on every push/PR)
├── data/                    # CSV data files (generated)
│   ├── payers.csv
│   ├── patients.csv
│   ├── providers.csv
│   ├── encounters.csv
│   ├── charges.csv
│   ├── claims.csv
│   ├── payments.csv
│   ├── denials.csv
│   ├── adjustments.csv
│   └── operating_costs.csv
├── src/
│   ├── __init__.py
│   ├── data_loader.py       # Data loading and type conversion
│   ├── metrics.py           # RCM metric calculation engine
│   └── validators.py        # Data integrity validation checks
└── tests/
    └── test_metrics.py      # Unit tests for metric functions
```

## Metrics Reference

| # | Metric | Formula | Benchmark |
|---|--------|---------|-----------|
| 1 | Days in A/R (DAR) | AR Balance / Avg Daily Charges | ≤ 35 days |
| 2 | Net Collection Rate (NCR) | Payments / (Charges − Contractual Adj) | ≥ 95% |
| 3 | Gross Collection Rate (GCR) | Total Payments / Total Charges | ≥ 50% |
| 4 | Clean Claim Rate (CCR) | Clean Claims / Total Claims | ≥ 90% |
| 5 | Denial Rate | Denied Claims / Total Claims | ≤ 10% |
| 6 | Denial Reasons Breakdown | Grouped by reason code with recovery rate | — |
| 7 | First-Pass Resolution Rate | Paid on First Submission / Total Claims | ≥ 85% |
| 8 | Charge Lag | Avg days from service date to post date | ≤ 3 days |
| 9 | Cost to Collect | Total RCM Costs / Total Collections | ≤ 5% |
| 10 | A/R Aging Buckets | Outstanding AR by 0-30, 31-60, 61-90, 91-120, 120+ days | — |
| 11 | Payment Accuracy Rate | Accurate Payments / Total Payments | ≥ 98% |
| 12 | Bad Debt Rate | Bad Debt Write-offs / Total Charges | ≤ 2% |
| 13 | Appeal Success Rate | Won Appeals / Total Appealed | — |
| 14 | Avg Reimbursement per Encounter | Total Payments / Total Encounters | — |
| 15 | Payer Mix Analysis | Revenue and volume by payer | — |
| 16 | Denial Rate by Payer | Denials per payer / Total payer claims | — |
| 17 | Department Performance | Revenue, collection rate, encounters by department | — |

## Dashboard Tabs

### Tab 1 — Executive Summary
A single-screen scorecard for leadership. Eight color-coded KPI cards (green/amber/red vs. industry benchmarks) cover Days in A/R, Net Collection Rate, Clean Claim Rate, Denial Rate, Gross Collection Rate, First-Pass Rate, Payment Accuracy, and Bad Debt Rate. Below the cards, trend lines for Days in A/R and Net Collection Rate give a month-over-month view, followed by a grouped bar chart of monthly encounter and claim volume.

### Tab 2 — Collections & Revenue
Breaks down where revenue is gained and lost. A waterfall chart traces the path from gross charges through contractual adjustments, net denials, and actual collections to arrive at net revenue. Supporting charts show gross vs. net collection rate trends over time, cost-to-collect trend vs. the 5% target, and average reimbursement per claim by month. A financial summary table at the bottom shows totals for charges, payments, adjustments, bad debt, and net revenue — with CSV/Excel export.

### Tab 3 — Claims & Denials
Focuses on claim quality and denial management. The claim status donut chart shows the split between Paid, Denied, Appealed, and Pending. A horizontal bar chart ranks the top denial reasons by volume and dollar amount denied. Trend lines track denial rate, clean claim rate, and first-pass rate month over month against their benchmarks. A charge lag histogram shows how many days elapse between service and charge posting. An expandable table lists every denial reason code with count, dollars denied, dollars recovered, and recovery rate — exportable to CSV/Excel.

### Tab 4 — A/R Aging & Cash Flow
Monitors outstanding receivables and cash timing. The aging bucket bar and pie charts show how much A/R sits in each age band (0–30, 31–60, 61–90, 91–120, 120+ days). A dual-axis chart overlays the raw A/R balance (bars) with Days in A/R (line) and a 35-day benchmark. The monthly cash flow chart compares charges vs. payments and plots the net cash flow line. An expandable aging detail table is exportable.

### Tab 5 — Payer Analysis
Compares performance across all contracted payers. Bar and pie charts show revenue and claim volume by payer; horizontal bar charts rank payers by collection rate and denial rate. A comparison table summarizes claims, charges, payments, collection rate, and denial rate side by side.

**Payer Drill-Down:** Select any payer from the dropdown to see that payer's claim count, total charges, total payments, and denied claim count. A claim status pie and denial reasons bar chart load for that payer, and an expandable claim-level table lists every claim with its charge amount, payment amount, and status — with export.

### Tab 6 — Department Performance
Evaluates clinical departments on revenue and efficiency. Grouped bars compare charges vs. payments by department; a horizontal bar ranks departments by collection rate; a pie shows encounter volume share; and a horizontal bar shows average payment per encounter. A stacked bar breaks down encounter types within each department.

**Department Drill-Down:** Select any department from the dropdown to see encounter count, claim count, total charges, and total payments for that department. An encounter type pie and claim status bar load for that department, and an expandable table lists every encounter with its linked claim, charge amount, payment amount, and status — with export.

## Dashboard Filters

All visualizations respond to the sidebar filters in real time:

- **Date Range** — Filter by date of service
- **Payer** — Filter to a specific payer or view all
- **Department** — Filter to a specific clinical department
- **Encounter Type** — Outpatient, Inpatient, Emergency, Telehealth

## Data Quality Validation

On startup, `src/validators.py` runs six checks against the loaded data:

| Check | Level |
|-------|-------|
| Negative monetary amounts | Warning |
| Orphaned foreign keys (e.g. payments referencing missing claims) | Warning |
| Null values in required columns | Error |
| Dates outside the 2020–2030 range | Warning |
| Unexpected claim status values | Warning |
| Null values in boolean columns | Warning |

Any issues appear in a collapsible **Data Quality** panel in the sidebar. Errors expand automatically; warnings are collapsed by default.

## Running with Docker

```bash
docker build -t rcm-analytics .
docker run -p 8501:8501 rcm-analytics
```

The app will be available at `http://localhost:8501`.

## CI / Continuous Integration

A GitHub Actions workflow (`.github/workflows/test.yml`) runs automatically on every push and pull request:

1. Checks out the code
2. Sets up Python 3.11 with pip caching
3. Installs dependencies
4. Generates sample data
5. Runs `pytest tests/ -v`

## Running Tests

```bash
pytest tests/ -v
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| streamlit | ≥ 1.30.0 | Web framework and UI |
| pandas | ≥ 2.0.0 | Data manipulation |
| plotly | ≥ 5.18.0 | Interactive visualizations |
| numpy | ≥ 1.24.0 | Numerical calculations |
| openpyxl | ≥ 3.1.0 | Excel export support |
| pytest | ≥ 7.0.0 | Unit testing (dev) |
