# Healthcare RCM Analytics Dashboard

A comprehensive Streamlit web application for monitoring and analyzing Healthcare Revenue Cycle Management (RCM) KPIs and metrics.

## Overview

This dashboard provides healthcare organizations with interactive visualizations across six key domains:

- **Executive Summary** — High-level KPI scorecard with benchmarks
- **Collections & Revenue** — Revenue waterfall, collection rate trends, cost to collect
- **Claims & Denials** — Denial reasons, clean claim rate, first-pass resolution, charge lag
- **A/R Aging & Cash Flow** — Aging buckets, days in A/R trend, monthly cash flow
- **Payer Analysis** — Revenue/volume/denial rate breakdown by payer
- **Department Performance** — Revenue, collection rate, and encounter volume by department

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
│   └── metrics.py           # RCM metric calculation engine
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

## Dashboard Filters

All visualizations respond to the sidebar filters:

- **Date Range** — Filter by date of service
- **Payer** — Filter to a specific payer or view all
- **Department** — Filter to a specific clinical department
- **Encounter Type** — Outpatient, Inpatient, Emergency, Telehealth

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| streamlit | ≥ 1.30.0 | Web framework and UI |
| pandas | ≥ 2.0.0 | Data manipulation |
| plotly | ≥ 5.18.0 | Interactive visualizations |
| numpy | ≥ 1.24.0 | Numerical calculations |
