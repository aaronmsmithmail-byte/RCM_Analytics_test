"""
Healthcare Revenue Cycle Management (RCM) Analytics Dashboard
=============================================================

This is the main Streamlit application that provides an interactive,
multi-tab dashboard for monitoring healthcare revenue cycle KPIs.

Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                    Streamlit App (app.py)                 │
    │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │
    │  │  Tab 1   │  │  Tab 2   │  │  Tab 3   │  │ Tab 4-6 │  │
    │  │ Summary  │  │ Revenue  │  │ Claims   │  │  More   │  │
    │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬────┘  │
    │       └──────────────┴─────────────┴─────────────┘       │
    │                         │                                 │
    │              ┌──────────▼──────────┐                     │
    │              │ Metrics Engine      │                     │
    │              │ (src/metrics.py)    │                     │
    │              └──────────┬──────────┘                     │
    │              ┌──────────▼──────────┐                     │
    │              │ Data Loader         │                     │
    │              │ (src/data_loader.py)│                     │
    │              └──────────┬──────────┘                     │
    │              ┌──────────▼──────────┐                     │
    │              │ SQLite Database     │                     │
    │              │ (data/*.db)         │                     │
    │              └─────────────────────┘                     │
    └──────────────────────────────────────────────────────────┘

Dashboard Tabs:
    1. Executive Summary  — 8 KPI scorecards + key trends + volume
    2. Collections & Revenue — Revenue waterfall, collection trends, cost analysis
    3. Claims & Denials   — Denial analysis, clean claims, charge lag, appeals
    4. A/R Aging & Cash   — Aging buckets, DAR trend, monthly cash flow
    5. Payer Analysis     — Revenue by payer, denial rates, payer comparison
    6. Department Perf.   — Revenue by department, encounter mix

How Streamlit Works (for educational purposes):
    - Streamlit reruns this entire script top-to-bottom on every user interaction
      (filter change, tab switch, etc.).
    - @st.cache_data prevents reloading data from the database on every rerun.
    - Sidebar widgets (selectbox, date_input) return the user's current selection.
    - st.tabs() creates a tabbed interface; content under each `with tab:` block
      only renders when that tab is active.

Running the Dashboard:
    pip install -r requirements.txt
    streamlit run app.py
"""

import io

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Import our custom modules
from src.data_loader import load_all_data       # Loads all tables from SQLite
from src.validators import validate_all         # Data integrity checks
from src.metadata_pages import (               # Four supplemental metadata pages
    render_data_lineage,
    render_data_catalog,
    render_knowledge_graph,
    render_semantic_layer,
)
from src.metrics import (                        # 17 SQL-based KPI query functions
    FilterParams,
    query_days_in_ar,
    query_net_collection_rate,
    query_gross_collection_rate,
    query_clean_claim_rate,
    query_denial_rate,
    query_denial_reasons,
    query_first_pass_rate,
    query_charge_lag,
    query_cost_to_collect,
    query_ar_aging,
    query_payment_accuracy,
    query_bad_debt_rate,
    query_appeal_success_rate,
    query_avg_reimbursement,
    query_payer_mix,
    query_denial_rate_by_payer,
    query_department_performance,
)

# ── Page Config ──────────────────────────────────────────────────────
# set_page_config() MUST be the first Streamlit command in the script.
# It configures the browser tab title, favicon, and default layout.
# "wide" layout uses the full browser width instead of a centered column.
st.set_page_config(
    page_title="Healthcare RCM Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "active_page" not in st.session_state:
    st.session_state["active_page"] = "dashboard"

# ── Custom CSS ───────────────────────────────────────────────────────
# Streamlit allows injecting custom CSS via st.markdown with unsafe_allow_html.
# We use this to create color-coded KPI cards:
#   - Green gradient (metric-good): KPI is meeting/exceeding benchmarks
#   - Yellow/Orange gradient (metric-warn): KPI needs attention
#   - Red gradient (metric-bad): KPI is critical and needs immediate action
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin-bottom: 10px;
    }
    .metric-card h2 { margin: 0; font-size: 2.2rem; }
    .metric-card p { margin: 5px 0 0 0; font-size: 0.9rem; opacity: 0.9; }
    .metric-good { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
    .metric-warn { background: linear-gradient(135deg, #f2994a 0%, #f2c94c 100%); }
    .metric-bad { background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }
    .benchmark-text { font-size: 0.75rem; opacity: 0.8; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)


def metric_card(label, value, benchmark="", status="neutral"):
    """
    Render a styled KPI card with color coding based on performance status.

    Args:
        label:     KPI name (e.g., "Days in A/R")
        value:     KPI value to display (e.g., "32.5")
        benchmark: Industry benchmark text (e.g., "Benchmark: < 35 days")
        status:    "good", "warn", "bad", or "neutral" — controls card color
    """
    css_class = {
        "good": "metric-card metric-good",
        "warn": "metric-card metric-warn",
        "bad": "metric-card metric-bad",
    }.get(status, "metric-card")
    bench_html = f'<p class="benchmark-text">{benchmark}</p>' if benchmark else ""
    st.markdown(
        f'<div class="{css_class}"><h2>{value}</h2><p>{label}</p>{bench_html}</div>',
        unsafe_allow_html=True,
    )


# ── Export Helpers ───────────────────────────────────────────────────
def df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def dfs_to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Write multiple DataFrames to an in-memory Excel file, one sheet each."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return buf.getvalue()


def export_buttons(label: str, sheets: dict[str, pd.DataFrame]):
    """Render side-by-side CSV and Excel download buttons."""
    primary_df = next(iter(sheets.values()))
    col_csv, col_xlsx, _ = st.columns([1, 1, 4])
    with col_csv:
        st.download_button(
            label="Download CSV",
            data=df_to_csv(primary_df),
            file_name=f"{label}.csv",
            mime="text/csv",
        )
    with col_xlsx:
        st.download_button(
            label="Download Excel",
            data=dfs_to_excel(sheets),
            file_name=f"{label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ── Load Data ────────────────────────────────────────────────────────
# @st.cache_data is a Streamlit decorator that caches the return value.
# On the first run, it calls load_all_data() (which queries SQLite).
# On subsequent reruns (user interactions), it returns the cached result
# instantly. This is essential for performance — without caching, every
# filter change would re-query the entire database.
#
# The cache is invalidated when:
#   - The function code changes
#   - The app is restarted
#   - You call st.cache_data.clear()
@st.cache_data
def get_data():
    """Load all RCM data from SQLite (cached after first call)."""
    return load_all_data()


try:
    data = get_data()
except FileNotFoundError as e:
    st.error(f"**Data files not found.** {e}")
    st.info("Run `python generate_sample_data.py` from the project root to create the required data files.")
    st.stop()
except ValueError as e:
    st.error(f"**Data validation error.** {e}")
    st.stop()
except Exception as e:
    st.error(f"**Unexpected error loading data:** {e}")
    st.stop()
claims = data["claims"]
payments = data["payments"]
denials = data["denials"]
adjustments = data["adjustments"]
encounters = data["encounters"]
charges = data["charges"]
payers = data["payers"]
operating_costs = data["operating_costs"]

# ── Data Validation ───────────────────────────────────────────────────
_validation_issues = validate_all()   # reads directly from Silver tables

# ── Sidebar Filters ─────────────────────────────────────────────────
# Sidebar filters allow users to slice data interactively. The filter
# cascade works as follows:
#   1. Date range -> filters claims and encounters by date_of_service
#   2. Payer -> filters claims to a specific insurance company
#   3. Department -> filters encounters (and thus claims) by dept
#   4. Encounter Type -> filters by visit type (outpatient, ED, etc.)
#
# All filters are applied BEFORE any metrics are calculated, so the
# KPIs always reflect the filtered subset of data.
st.sidebar.title("Filters")

# Date range filter — lets users focus on a specific time period
min_date = claims["date_of_service"].min().date()
max_date = claims["date_of_service"].max().date()
date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Payer filter
payer_options = ["All"] + sorted(payers["payer_name"].tolist())
selected_payer = st.sidebar.selectbox("Payer", payer_options)

# Department filter
dept_options = ["All"] + sorted(encounters["department"].unique().tolist())
selected_dept = st.sidebar.selectbox("Department", dept_options)

# Encounter type filter
enc_type_options = ["All"] + sorted(encounters["encounter_type"].unique().tolist())
selected_enc_type = st.sidebar.selectbox("Encounter Type", enc_type_options)

# ── Apply Filters ────────────────────────────────────────────────────
# The filtering strategy: start with claims (the central table) and filter
# outward to related tables. This ensures all metrics use consistent data.
# We use .copy() to avoid pandas SettingWithCopyWarning.
if len(date_range) == 2:
    start_dt, end_dt = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    start_dt, end_dt = pd.Timestamp(min_date), pd.Timestamp(max_date)

# Filter claims by date
f_claims = claims[(claims["date_of_service"] >= start_dt) & (claims["date_of_service"] <= end_dt)].copy()

# Filter by payer
if selected_payer != "All":
    payer_id = payers[payers["payer_name"] == selected_payer]["payer_id"].values[0]
    f_claims = f_claims[f_claims["payer_id"] == payer_id]

# Filter encounters by department and type
f_encounters = encounters[
    (encounters["date_of_service"] >= start_dt) & (encounters["date_of_service"] <= end_dt)
].copy()
if selected_dept != "All":
    f_encounters = f_encounters[f_encounters["department"] == selected_dept]
    enc_ids = f_encounters["encounter_id"].unique()
    f_claims = f_claims[f_claims["encounter_id"].isin(enc_ids)]
if selected_enc_type != "All":
    f_encounters = f_encounters[f_encounters["encounter_type"] == selected_enc_type]
    enc_ids = f_encounters["encounter_id"].unique()
    f_claims = f_claims[f_claims["encounter_id"].isin(enc_ids)]

# Filter related tables by cascading from filtered claims.
# These filtered DataFrames are used for drill-down sections and direct
# DataFrame aggregations (waterfall, cash flow, claim status pie, etc.).
claim_ids = f_claims["claim_id"].unique()
f_payments = payments[payments["claim_id"].isin(claim_ids)].copy()
f_denials = denials[denials["claim_id"].isin(claim_ids)].copy()
f_adjustments = adjustments[adjustments["claim_id"].isin(claim_ids)].copy()
f_charges = charges[charges["encounter_id"].isin(f_encounters["encounter_id"].unique())].copy()

# ── Build FilterParams for SQL-based metric queries ──────────────────
# All 17 metric query_* functions accept a FilterParams object that
# encodes the same four sidebar dimensions as SQL WHERE clause parameters.
_payer_id = (
    payers[payers["payer_name"] == selected_payer]["payer_id"].values[0]
    if selected_payer != "All" else None
)
params = FilterParams(
    start_date=str(start_dt.date()),
    end_date=str(end_dt.date()),
    payer_id=_payer_id,
    department=selected_dept if selected_dept != "All" else None,
    encounter_type=selected_enc_type if selected_enc_type != "All" else None,
)

# ── Metadata navigation (sidebar) ────────────────────────────────────
# These buttons must render BEFORE the page router so they appear on
# every page, including metadata pages that call st.stop() early.
st.sidebar.divider()
st.sidebar.markdown("### Metadata")
if st.sidebar.button("Data Catalog", use_container_width=True):
    st.session_state["active_page"] = "data_catalog"
if st.sidebar.button("Data Lineage", use_container_width=True):
    st.session_state["active_page"] = "data_lineage"
if st.sidebar.button("Knowledge Graph", use_container_width=True):
    st.session_state["active_page"] = "knowledge_graph"
if st.sidebar.button("Semantic Layer", use_container_width=True):
    st.session_state["active_page"] = "semantic_layer"
if st.session_state["active_page"] != "dashboard":
    if st.sidebar.button("Back to Dashboard", type="primary", use_container_width=True):
        st.session_state["active_page"] = "dashboard"

# ── Page router ──────────────────────────────────────────────────────
_active = st.session_state["active_page"]
if _active == "data_catalog":
    render_data_catalog()
    st.stop()
elif _active == "data_lineage":
    render_data_lineage()
    st.stop()
elif _active == "knowledge_graph":
    render_knowledge_graph()
    st.stop()
elif _active == "semantic_layer":
    render_semantic_layer()
    st.stop()

# ── Header ───────────────────────────────────────────────────────────
st.title("Healthcare RCM Analytics Dashboard")
st.caption(f"Analyzing {len(f_claims):,} claims | {len(f_encounters):,} encounters | Date range: {start_dt.strftime('%b %Y')} to {end_dt.strftime('%b %Y')}")

if f_claims.empty:
    st.warning("No claims match the selected filters. Adjust the sidebar filters to see data.")
    st.stop()

# ── Tabs ─────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Executive Summary",
    "Collections & Revenue",
    "Claims & Denials",
    "A/R Aging & Cash Flow",
    "Payer Analysis",
    "Department Performance",
])

# =====================================================================
# TAB 1: EXECUTIVE SUMMARY
# =====================================================================
# The Executive Summary provides a "single pane of glass" view of the
# 8 most critical RCM KPIs. Each KPI card is color-coded:
#   Green  = Meeting industry benchmark
#   Yellow = Approaching danger zone
#   Red    = Below benchmark, needs immediate attention
#
# Below the KPI cards, trend charts show how DAR and NCR are tracking
# over time, with benchmark lines for reference.
# =====================================================================
with tab1:
    st.header("Executive Summary")

    # Calculate all KPIs via parameterized SQL queries against the Silver layer
    dar_val, dar_trend = query_days_in_ar(params)
    ncr_val, ncr_trend = query_net_collection_rate(params)
    gcr_val, gcr_trend = query_gross_collection_rate(params)
    ccr_val, ccr_trend = query_clean_claim_rate(params)
    denial_val, denial_trend = query_denial_rate(params)
    fpr_val, fpr_trend = query_first_pass_rate(params)
    accuracy_val = query_payment_accuracy(params)
    bad_debt_val, bad_debt_amt, total_charges = query_bad_debt_rate(params)

    # Top-level KPI cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        status = "good" if dar_val < 35 else ("warn" if dar_val < 50 else "bad")
        metric_card("Days in A/R", f"{dar_val}", "Benchmark: < 35 days", status)
    with col2:
        status = "good" if ncr_val > 95 else ("warn" if ncr_val > 90 else "bad")
        metric_card("Net Collection Rate", f"{ncr_val}%", "Benchmark: > 95%", status)
    with col3:
        status = "good" if ccr_val > 90 else ("warn" if ccr_val > 80 else "bad")
        metric_card("Clean Claim Rate", f"{ccr_val}%", "Benchmark: > 90%", status)
    with col4:
        status = "good" if denial_val < 10 else ("warn" if denial_val < 15 else "bad")
        metric_card("Denial Rate", f"{denial_val}%", "Benchmark: < 10%", status)

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        status = "good" if gcr_val > 70 else ("warn" if gcr_val > 55 else "bad")
        metric_card("Gross Collection Rate", f"{gcr_val}%", "Benchmark: > 70%", status)
    with col6:
        status = "good" if fpr_val > 85 else ("warn" if fpr_val > 75 else "bad")
        metric_card("First-Pass Rate", f"{fpr_val}%", "Benchmark: > 85%", status)
    with col7:
        status = "good" if accuracy_val > 95 else ("warn" if accuracy_val > 90 else "bad")
        metric_card("Payment Accuracy", f"{accuracy_val}%", "Benchmark: > 95%", status)
    with col8:
        status = "good" if bad_debt_val < 3 else ("warn" if bad_debt_val < 5 else "bad")
        metric_card("Bad Debt Rate", f"{bad_debt_val}%", "Benchmark: < 3%", status)

    st.divider()

    # Key metric trends
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Days in A/R Trend")
        fig = px.line(dar_trend.reset_index(), x="year_month", y="days_in_ar",
                      labels={"year_month": "Month", "days_in_ar": "Days in A/R"})
        fig.add_hline(y=35, line_dash="dash", line_color="green", annotation_text="Benchmark: 35 days")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Net Collection Rate Trend")
        fig = px.line(ncr_trend.reset_index(), x="year_month", y="ncr",
                      labels={"year_month": "Month", "ncr": "NCR (%)"})
        fig.add_hline(y=95, line_dash="dash", line_color="green", annotation_text="Benchmark: 95%")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Volume summary
    st.subheader("Monthly Volume")
    enc_monthly = f_encounters.copy()
    enc_monthly["year_month"] = enc_monthly["date_of_service"].dt.to_period("M").astype(str)
    vol = enc_monthly.groupby("year_month").agg(
        encounters=("encounter_id", "count")
    ).reset_index()
    claims_monthly = f_claims.copy()
    claims_monthly["year_month"] = claims_monthly["date_of_service"].dt.to_period("M").astype(str)
    claims_vol = claims_monthly.groupby("year_month")["claim_id"].count().reset_index()
    claims_vol.columns = ["year_month", "claims"]
    vol = vol.merge(claims_vol, on="year_month", how="outer").fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=vol["year_month"], y=vol["encounters"], name="Encounters", marker_color="#667eea"))
    fig.add_trace(go.Bar(x=vol["year_month"], y=vol["claims"], name="Claims", marker_color="#764ba2"))
    fig.update_layout(barmode="group", height=350, margin=dict(t=30, b=30),
                      xaxis_title="Month", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)


# =====================================================================
# TAB 2: COLLECTIONS & REVENUE
# =====================================================================
# This tab focuses on the financial health of the revenue cycle:
#   - Revenue Waterfall: Shows how charges flow to net revenue (charges
#     minus adjustments, denials, resulting in net collections)
#   - Collection rate trends: GCR and NCR over time
#   - Cost to Collect: How efficient is the billing operation?
#   - Average reimbursement: Revenue per claim over time
# =====================================================================
with tab2:
    st.header("Collections & Revenue Analysis")

    gcr_val, gcr_trend = query_gross_collection_rate(params)
    ncr_val, ncr_trend = query_net_collection_rate(params)
    ctc_val, ctc_trend = query_cost_to_collect(params)
    avg_reimb, reimb_trend = query_avg_reimbursement(params)
    bad_debt_val, bad_debt_amt, total_charges_val = query_bad_debt_rate(params)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Gross Collection Rate", f"{gcr_val}%")
    with col2:
        st.metric("Net Collection Rate", f"{ncr_val}%")
    with col3:
        st.metric("Cost to Collect", f"{ctc_val}%", help="Benchmark: 3-8%")
    with col4:
        st.metric("Avg Reimbursement / Claim", f"${avg_reimb:,.2f}")

    st.divider()

    # Revenue waterfall
    st.subheader("Revenue Waterfall")
    total_charges_w = f_claims["total_charge_amount"].sum()
    total_payments_w = f_payments["payment_amount"].sum()
    total_adj_w = f_adjustments["adjustment_amount"].sum()
    total_denials_w = f_denials["denied_amount"].sum()
    net_revenue = total_payments_w

    fig = go.Figure(go.Waterfall(
        name="Revenue Flow",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "total"],
        x=["Total Charges", "Adjustments", "Denials (Lost)", "Collections", "Net Revenue"],
        y=[total_charges_w, -total_adj_w, -(total_denials_w - f_denials["recovered_amount"].sum()),
           total_payments_w - total_charges_w + total_adj_w + (total_denials_w - f_denials["recovered_amount"].sum()),
           0],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        text=[f"${total_charges_w:,.0f}", f"-${total_adj_w:,.0f}",
              f"-${(total_denials_w - f_denials['recovered_amount'].sum()):,.0f}",
              "", f"${net_revenue:,.0f}"],
        textposition="outside",
    ))
    fig.update_layout(height=400, margin=dict(t=30, b=30), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Collection rate trends
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Collection Rates Over Time")
        combined_trend = gcr_trend[["gcr"]].join(ncr_trend[["ncr"]], how="outer").fillna(0).reset_index()
        combined_trend.columns = ["Month", "Gross Collection Rate", "Net Collection Rate"]
        fig = px.line(combined_trend, x="Month", y=["Gross Collection Rate", "Net Collection Rate"])
        fig.update_layout(height=350, margin=dict(t=30, b=30), yaxis_title="%")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Cost to Collect Trend")
        fig = px.area(ctc_trend.reset_index(), x="year_month", y="cost_to_collect_pct",
                      labels={"year_month": "Month", "cost_to_collect_pct": "Cost to Collect (%)"})
        fig.add_hline(y=5, line_dash="dash", line_color="green", annotation_text="Target: 5%")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Avg reimbursement trend
    st.subheader("Average Reimbursement per Claim")
    fig = px.bar(reimb_trend.reset_index(), x="year_month", y="payment_amount",
                 labels={"year_month": "Month", "payment_amount": "Avg Reimbursement ($)"})
    fig.update_layout(height=300, margin=dict(t=30, b=30))
    st.plotly_chart(fig, use_container_width=True)

    # Financial summary
    st.subheader("Financial Summary")
    fin_data = {
        "Metric": ["Total Charges", "Total Payments", "Total Adjustments",
                    "Bad Debt Write-offs", "Net Revenue"],
        "Amount": [
            f"${total_charges_w:,.2f}",
            f"${total_payments_w:,.2f}",
            f"${total_adj_w:,.2f}",
            f"${bad_debt_amt:,.2f}",
            f"${net_revenue:,.2f}",
        ]
    }
    fin_df = pd.DataFrame(fin_data)
    st.dataframe(fin_df, hide_index=True, use_container_width=True)
    export_buttons("collections_revenue", {
        "Financial Summary": fin_df,
        "Filtered Claims": f_claims,
        "Filtered Payments": f_payments,
        "Filtered Adjustments": f_adjustments,
    })


# =====================================================================
# TAB 3: CLAIMS & DENIALS
# =====================================================================
# This tab helps identify and fix billing process problems:
#   - Clean Claim Rate: Are we submitting error-free claims?
#   - Denial Rate & Reasons: Why are claims being rejected?
#   - First-Pass Rate: Are claims being paid on first submission?
#   - Charge Lag: How quickly are services being billed?
#   - Appeal Success: Are we recovering revenue from denied claims?
#
# The denial reasons bar chart is the most actionable visualization —
# it shows exactly where to focus process improvement efforts.
# =====================================================================
with tab3:
    st.header("Claims & Denials Analysis")

    ccr_val, ccr_trend = query_clean_claim_rate(params)
    denial_val, denial_trend = query_denial_rate(params)
    fpr_val, fpr_trend = query_first_pass_rate(params)
    denial_reasons = query_denial_reasons(params)
    charge_lag_val, charge_lag_trend, charge_lag_dist = query_charge_lag(params)
    appeal_rate, total_appealed, won_appeals = query_appeal_success_rate(params)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Clean Claim Rate", f"{ccr_val}%", help="Benchmark: > 90%")
    with col2:
        st.metric("Denial Rate", f"{denial_val}%", help="Benchmark: < 10%")
    with col3:
        st.metric("First-Pass Rate", f"{fpr_val}%", help="Benchmark: > 85%")
    with col4:
        st.metric("Appeal Success Rate", f"{appeal_rate}%", help=f"{won_appeals} won of {total_appealed} appealed")

    st.divider()

    # Claims status distribution
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Claim Status Distribution")
        status_counts = f_claims["claim_status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        fig = px.pie(status_counts, values="Count", names="Status",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Top Denial Reasons")
        fig = px.bar(denial_reasons.head(10), x="count", y="denial_reason_description",
                     orientation="h", color="total_denied_amount",
                     color_continuous_scale="Reds",
                     labels={"count": "Denial Count", "denial_reason_description": "Reason",
                             "total_denied_amount": "$ Denied"})
        fig.update_layout(height=350, margin=dict(t=30, b=30), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    # Denial & Clean Claim trends
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Denial Rate Trend")
        fig = px.line(denial_trend.reset_index(), x="year_month", y="denial_rate",
                      labels={"year_month": "Month", "denial_rate": "Denial Rate (%)"})
        fig.add_hline(y=10, line_dash="dash", line_color="green", annotation_text="Target: 10%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        st.subheader("Clean Claim Rate Trend")
        fig = px.line(ccr_trend.reset_index(), x="year_month", y="ccr",
                      labels={"year_month": "Month", "ccr": "Clean Claim Rate (%)"})
        fig.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="Target: 90%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Charge lag
    col_left3, col_right3 = st.columns(2)
    with col_left3:
        st.subheader(f"Charge Lag Distribution (Avg: {charge_lag_val} days)")
        lag_df = charge_lag_dist.reset_index()
        lag_df.columns = ["Days", "Count"]
        lag_df = lag_df[lag_df["Days"] <= 30]
        fig = px.bar(lag_df, x="Days", y="Count",
                     labels={"Days": "Lag (Days)", "Count": "# of Charges"})
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right3:
        st.subheader("First-Pass Rate Trend")
        fig = px.line(fpr_trend.reset_index(), x="year_month", y="fpr",
                      labels={"year_month": "Month", "fpr": "First-Pass Rate (%)"})
        fig.add_hline(y=85, line_dash="dash", line_color="green", annotation_text="Target: 85%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Denial details table
    with st.expander("Denial Reasons Detail Table"):
        denial_detail_df = denial_reasons[["denial_reason_code", "denial_reason_description", "count",
                        "total_denied_amount", "total_recovered", "recovery_rate"]].round(2)
        st.dataframe(denial_detail_df, hide_index=True, use_container_width=True)

    export_buttons("claims_denials", {
        "Denial Reasons": denial_detail_df,
        "Filtered Claims": f_claims,
        "Filtered Denials": f_denials,
    })


# =====================================================================
# TAB 4: A/R AGING & CASH FLOW
# =====================================================================
# This tab monitors cash flow and the age of unpaid balances:
#   - A/R Aging Buckets: How old are unpaid balances? (0-30, 31-60,
#     61-90, 91-120, 120+ days). Older = harder to collect.
#   - Days in A/R Trend: Dual-axis chart showing A/R balance (bars)
#     and DAR metric (line) over time.
#   - Monthly Cash Flow: Charges vs. payments per month, with net
#     cash flow line showing whether the practice is cash-positive.
# =====================================================================
with tab4:
    st.header("Accounts Receivable Aging & Cash Flow")

    dar_val, dar_trend = query_days_in_ar(params)
    aging_summary, total_ar = query_ar_aging(params)

    col1, col2, col3 = st.columns(3)
    with col1:
        status = "good" if dar_val < 35 else ("warn" if dar_val < 50 else "bad")
        metric_card("Days in A/R", f"{dar_val}", "Benchmark: < 35 days", status)
    with col2:
        metric_card("Total Outstanding A/R", f"${total_ar:,.0f}", "", "neutral")
    with col3:
        pct_0_60 = aging_summary.loc[["0-30", "31-60"], "pct_of_total"].sum() if len(aging_summary) > 0 else 0
        status = "good" if pct_0_60 > 70 else ("warn" if pct_0_60 > 50 else "bad")
        metric_card("A/R in 0-60 Days", f"{pct_0_60:.1f}%", "Benchmark: > 70%", status)

    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("A/R Aging Buckets")
        aging_df = aging_summary.reset_index()
        aging_df.columns = ["Bucket", "Claim Count", "Total A/R", "% of Total"]
        fig = px.bar(aging_df, x="Bucket", y="Total A/R",
                     text="% of Total",
                     color="Bucket",
                     color_discrete_sequence=["#2ecc71", "#27ae60", "#f39c12", "#e74c3c", "#c0392b"])
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(height=400, margin=dict(t=30, b=30), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("A/R Aging Distribution")
        fig = px.pie(aging_df, values="Total A/R", names="Bucket",
                     color_discrete_sequence=["#2ecc71", "#27ae60", "#f39c12", "#e74c3c", "#c0392b"])
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # DAR trend
    st.subheader("Days in A/R Trend")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=dar_trend.reset_index()["year_month"], y=dar_trend["ar_balance"],
               name="A/R Balance", marker_color="#667eea", opacity=0.6),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=dar_trend.reset_index()["year_month"], y=dar_trend["days_in_ar"],
                   name="Days in A/R", line=dict(color="#e74c3c", width=3)),
        secondary_y=True,
    )
    fig.add_hline(y=35, line_dash="dash", line_color="green", secondary_y=True,
                  annotation_text="Target: 35 days")
    fig.update_layout(height=400, margin=dict(t=30, b=30))
    fig.update_yaxes(title_text="A/R Balance ($)", secondary_y=False)
    fig.update_yaxes(title_text="Days in A/R", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # Cash flow
    st.subheader("Monthly Cash Flow")
    claims_cf = f_claims.copy()
    claims_cf["year_month"] = claims_cf["date_of_service"].dt.to_period("M").astype(str)
    pay_cf = f_payments.merge(claims_cf[["claim_id", "year_month"]], on="claim_id", how="left")

    cf = pd.DataFrame({
        "charges": claims_cf.groupby("year_month")["total_charge_amount"].sum(),
        "payments": pay_cf.groupby("year_month")["payment_amount"].sum()
    }).fillna(0).reset_index()
    cf.columns = ["Month", "Charges", "Payments"]
    cf["Net Cash Flow"] = cf["Payments"] - cf["Charges"]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=cf["Month"], y=cf["Charges"], name="Charges", marker_color="#667eea"))
    fig.add_trace(go.Bar(x=cf["Month"], y=cf["Payments"], name="Payments", marker_color="#2ecc71"))
    fig.add_trace(go.Scatter(x=cf["Month"], y=cf["Net Cash Flow"], name="Net Cash Flow",
                             line=dict(color="#e74c3c", width=2, dash="dot")))
    fig.update_layout(barmode="group", height=400, margin=dict(t=30, b=30),
                      yaxis_title="Amount ($)")
    st.plotly_chart(fig, use_container_width=True)

    # A/R aging table
    with st.expander("A/R Aging Detail"):
        aging_detail = aging_df.copy()
        aging_detail["Total A/R"] = aging_detail["Total A/R"].apply(lambda x: f"${x:,.2f}")
        aging_detail["% of Total"] = aging_detail["% of Total"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(aging_detail, hide_index=True, use_container_width=True)

    export_buttons("ar_aging_cashflow", {
        "AR Aging Summary": aging_df,
        "Cash Flow": cf,
    })


# =====================================================================
# TAB 5: PAYER ANALYSIS
# =====================================================================
# This tab compares performance across insurance companies:
#   - Revenue by Payer: Which payers generate the most revenue?
#   - Payer Mix: Volume distribution across payers (pie chart)
#   - Collection Rate by Payer: Which payers pay best/worst?
#   - Denial Rate by Payer: Which payers deny most frequently?
#   - Payer Comparison Table: Side-by-side metrics for all payers
#
# This data is critical for payer contract negotiations and for
# prioritizing follow-up on underpaying or high-denial payers.
# =====================================================================
with tab5:
    st.header("Payer Performance Analysis")

    payer_mix = query_payer_mix(params)
    denial_by_payer = query_denial_rate_by_payer(params)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Payer")
        fig = px.bar(payer_mix, x="payer_name", y="total_payments",
                     color="payer_type",
                     labels={"payer_name": "Payer", "total_payments": "Total Payments ($)",
                             "payer_type": "Type"})
        fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Payer Mix (by Volume)")
        fig = px.pie(payer_mix, values="claim_count", names="payer_name",
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Collection rate by payer
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Collection Rate by Payer")
        fig = px.bar(payer_mix.sort_values("collection_rate"),
                     x="collection_rate", y="payer_name", orientation="h",
                     color="collection_rate",
                     color_continuous_scale="RdYlGn",
                     labels={"collection_rate": "Collection Rate (%)", "payer_name": "Payer"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        st.subheader("Denial Rate by Payer")
        fig = px.bar(denial_by_payer.sort_values("denial_rate"),
                     x="denial_rate", y="payer_name", orientation="h",
                     color="denial_rate",
                     color_continuous_scale="RdYlGn_r",
                     labels={"denial_rate": "Denial Rate (%)", "payer_name": "Payer"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Payer comparison table
    st.subheader("Payer Comparison Table")
    payer_table = payer_mix.merge(
        denial_by_payer[["payer_id", "denial_rate"]], on="payer_id", how="left"
    )[["payer_name", "payer_type", "claim_count", "total_charges", "total_payments",
       "collection_rate", "denial_rate"]].round(2)
    payer_table.columns = ["Payer", "Type", "Claims", "Total Charges", "Total Payments",
                           "Collection Rate (%)", "Denial Rate (%)"]
    st.dataframe(payer_table, hide_index=True, use_container_width=True)

    export_buttons("payer_analysis", {
        "Payer Comparison": payer_table,
        "Payer Mix": payer_mix,
        "Denial by Payer": denial_by_payer,
    })

    # Payer type summary
    st.subheader("Performance by Payer Type")
    type_summary = payer_mix.groupby("payer_type").agg(
        claims=("claim_count", "sum"),
        charges=("total_charges", "sum"),
        payments=("total_payments", "sum"),
    ).reset_index()
    type_summary["collection_rate"] = (type_summary["payments"] / type_summary["charges"] * 100).round(2)
    fig = px.bar(type_summary, x="payer_type", y=["charges", "payments"],
                 barmode="group",
                 labels={"value": "Amount ($)", "payer_type": "Payer Type", "variable": "Metric"})
    fig.update_layout(height=350, margin=dict(t=30, b=30))
    st.plotly_chart(fig, use_container_width=True)

    # ── Payer Drill-Down ──────────────────────────────────────────────
    st.divider()
    st.subheader("Payer Drill-Down")
    payer_names = sorted(payer_mix["payer_name"].tolist())
    selected_drilldown_payer = st.selectbox("Select a payer to inspect", payer_names, key="payer_drilldown")
    if selected_drilldown_payer:
        drill_payer_id = payers[payers["payer_name"] == selected_drilldown_payer]["payer_id"].values[0]
        drill_claims = f_claims[f_claims["payer_id"] == drill_payer_id].copy()
        drill_payments = f_payments[f_payments["claim_id"].isin(drill_claims["claim_id"])].copy()
        drill_denials = f_denials[f_denials["claim_id"].isin(drill_claims["claim_id"])].copy()

        kc1, kc2, kc3, kc4 = st.columns(4)
        with kc1:
            st.metric("Claims", f"{len(drill_claims):,}")
        with kc2:
            st.metric("Total Charges", f"${drill_claims['total_charge_amount'].sum():,.0f}")
        with kc3:
            st.metric("Total Payments", f"${drill_payments['payment_amount'].sum():,.0f}")
        with kc4:
            denied_count = drill_claims["claim_status"].isin(["Denied", "Appealed"]).sum()
            st.metric("Denied Claims", f"{denied_count:,}")

        col_d1, col_d2 = st.columns(2)
        with col_d1:
            status_counts = drill_claims["claim_status"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            fig = px.pie(status_counts, values="Count", names="Status",
                         title="Claim Status Mix",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
        with col_d2:
            if not drill_denials.empty:
                denial_reasons_drill = drill_denials["denial_reason_description"].value_counts().reset_index()
                denial_reasons_drill.columns = ["Reason", "Count"]
                fig = px.bar(denial_reasons_drill, x="Count", y="Reason", orientation="h",
                             title="Denial Reasons",
                             labels={"Count": "# Denials", "Reason": ""})
                fig.update_layout(height=300, margin=dict(t=40, b=10),
                                  yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No denials for this payer in the selected date range.")

        with st.expander("Claim-Level Detail"):
            claim_detail = drill_claims[["claim_id", "date_of_service", "submission_date",
                                          "total_charge_amount", "claim_status", "is_clean_claim"]].copy()
            pay_totals = drill_payments.groupby("claim_id")["payment_amount"].sum().reset_index()
            claim_detail = claim_detail.merge(pay_totals, on="claim_id", how="left")
            claim_detail["payment_amount"] = claim_detail["payment_amount"].fillna(0)
            claim_detail.columns = ["Claim ID", "Date of Service", "Submission Date",
                                     "Charge Amount", "Status", "Clean Claim", "Payment Amount"]
            st.dataframe(claim_detail, hide_index=True, use_container_width=True)
            export_buttons(f"payer_drilldown_{selected_drilldown_payer.replace(' ', '_')}", {
                "Claims": claim_detail,
                "Denials": drill_denials,
            })


# =====================================================================
# TAB 6: DEPARTMENT PERFORMANCE
# =====================================================================
# This tab breaks down revenue cycle performance by clinical department:
#   - Revenue by Department: Charges vs. payments for each dept
#   - Collection Rate: Which departments collect most effectively?
#   - Encounter Volume: How busy is each department?
#   - Avg Payment per Encounter: Revenue intensity by department
#   - Encounter Type Mix: Distribution of visit types per department
#
# This helps administrators identify departments that may need
# additional coding support, billing staff, or process improvement.
# =====================================================================
with tab6:
    st.header("Department Performance")

    dept_perf = query_department_performance(params)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Department")
        fig = px.bar(dept_perf, x="department", y=["total_charges", "total_payments"],
                     barmode="group",
                     labels={"value": "Amount ($)", "department": "Department", "variable": "Metric"})
        fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Collection Rate by Department")
        fig = px.bar(dept_perf.sort_values("collection_rate"),
                     x="collection_rate", y="department", orientation="h",
                     color="collection_rate",
                     color_continuous_scale="RdYlGn",
                     labels={"collection_rate": "Collection Rate (%)", "department": "Department"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Encounter volume by department
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Encounter Volume by Department")
        fig = px.pie(dept_perf, values="encounter_count", names="department",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        st.subheader("Avg Payment per Encounter")
        fig = px.bar(dept_perf.sort_values("avg_payment_per_encounter"),
                     x="avg_payment_per_encounter", y="department", orientation="h",
                     color="avg_payment_per_encounter",
                     color_continuous_scale="Viridis",
                     labels={"avg_payment_per_encounter": "Avg $/Encounter", "department": "Department"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    # Department encounter type breakdown
    st.subheader("Encounter Type by Department")
    dept_enc = f_encounters.groupby(["department", "encounter_type"]).size().reset_index(name="count")
    fig = px.bar(dept_enc, x="department", y="count", color="encounter_type",
                 labels={"count": "Count", "department": "Department", "encounter_type": "Type"})
    fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # Department table
    st.subheader("Department Performance Summary")
    dept_table = dept_perf.copy()
    dept_table["total_charges"] = dept_table["total_charges"].apply(lambda x: f"${x:,.2f}")
    dept_table["total_payments"] = dept_table["total_payments"].apply(lambda x: f"${x:,.2f}")
    dept_table["collection_rate"] = dept_table["collection_rate"].apply(lambda x: f"{x:.1f}%")
    dept_table["avg_payment_per_encounter"] = dept_table["avg_payment_per_encounter"].apply(lambda x: f"${x:,.2f}")
    dept_table.columns = ["Department", "Encounters", "Total Charges", "Total Payments",
                          "Collection Rate", "Avg $/Encounter"]
    st.dataframe(dept_table, hide_index=True, use_container_width=True)
    export_buttons("department_performance", {
        "Department Summary": dept_perf,
        "Encounter Type Mix": dept_enc,
    })

    # ── Department Drill-Down ─────────────────────────────────────────
    st.divider()
    st.subheader("Department Drill-Down")
    dept_names = sorted(dept_perf["department"].tolist())
    selected_drilldown_dept = st.selectbox("Select a department to inspect", dept_names, key="dept_drilldown")
    if selected_drilldown_dept:
        drill_encs = f_encounters[f_encounters["department"] == selected_drilldown_dept].copy()
        drill_enc_ids = drill_encs["encounter_id"].unique()
        drill_dept_claims = f_claims[f_claims["encounter_id"].isin(drill_enc_ids)].copy()
        drill_dept_payments = f_payments[f_payments["claim_id"].isin(drill_dept_claims["claim_id"])].copy()
        drill_dept_denials = f_denials[f_denials["claim_id"].isin(drill_dept_claims["claim_id"])].copy()

        kd1, kd2, kd3, kd4 = st.columns(4)
        with kd1:
            st.metric("Encounters", f"{len(drill_encs):,}")
        with kd2:
            st.metric("Claims", f"{len(drill_dept_claims):,}")
        with kd3:
            st.metric("Total Charges", f"${drill_dept_claims['total_charge_amount'].sum():,.0f}")
        with kd4:
            st.metric("Total Payments", f"${drill_dept_payments['payment_amount'].sum():,.0f}")

        col_dd1, col_dd2 = st.columns(2)
        with col_dd1:
            enc_type_counts = drill_encs["encounter_type"].value_counts().reset_index()
            enc_type_counts.columns = ["Type", "Count"]
            fig = px.pie(enc_type_counts, values="Count", names="Type",
                         title="Encounter Type Mix",
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
        with col_dd2:
            status_counts_dept = drill_dept_claims["claim_status"].value_counts().reset_index()
            status_counts_dept.columns = ["Status", "Count"]
            fig = px.bar(status_counts_dept, x="Status", y="Count",
                         title="Claim Status",
                         color="Status",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=300, margin=dict(t=40, b=10), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Encounter & Claim Detail"):
            enc_detail = drill_encs[["encounter_id", "date_of_service", "encounter_type",
                                      "patient_id", "provider_id"]].copy()
            enc_detail = enc_detail.merge(
                drill_dept_claims[["encounter_id", "claim_id", "total_charge_amount", "claim_status"]],
                on="encounter_id", how="left"
            )
            pay_totals_dept = drill_dept_payments.groupby("claim_id")["payment_amount"].sum().reset_index()
            enc_detail = enc_detail.merge(pay_totals_dept, on="claim_id", how="left")
            enc_detail["payment_amount"] = enc_detail["payment_amount"].fillna(0)
            enc_detail.columns = ["Encounter ID", "Date of Service", "Encounter Type",
                                   "Patient ID", "Provider ID", "Claim ID",
                                   "Charge Amount", "Claim Status", "Payment Amount"]
            st.dataframe(enc_detail, hide_index=True, use_container_width=True)
            export_buttons(f"dept_drilldown_{selected_drilldown_dept.replace(' ', '_')}", {
                "Encounters & Claims": enc_detail,
                "Denials": drill_dept_denials,
            })


# ── Sidebar Footer ───────────────────────────────────────────────────
# Show a summary of the filtered data volume in the sidebar so users
# always know how much data they're looking at.
st.sidebar.divider()
st.sidebar.markdown("### Data Summary")
st.sidebar.markdown(f"- **Patients:** {len(data['patients']):,}")
st.sidebar.markdown(f"- **Providers:** {len(data['providers']):,}")
st.sidebar.markdown(f"- **Encounters:** {len(f_encounters):,}")
st.sidebar.markdown(f"- **Claims:** {len(f_claims):,}")
st.sidebar.markdown(f"- **Payments:** {len(f_payments):,}")
st.sidebar.markdown(f"- **Denials:** {len(f_denials):,}")

if _validation_issues:
    st.sidebar.divider()
    errors = [i for i in _validation_issues if i["level"] == "error"]
    warnings = [i for i in _validation_issues if i["level"] == "warning"]
    with st.sidebar.expander(
        f"{'🔴' if errors else '🟡'} Data Quality ({len(_validation_issues)} issue{'s' if len(_validation_issues) != 1 else ''})",
        expanded=bool(errors),
    ):
        for issue in _validation_issues:
            icon = "🔴" if issue["level"] == "error" else "🟡"
            st.markdown(f"{icon} **{issue['table']}**: {issue['message']}")

st.sidebar.divider()
st.sidebar.caption("Healthcare RCM Analytics Dashboard v1.0")
