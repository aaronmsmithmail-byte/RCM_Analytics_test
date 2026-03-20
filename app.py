"""
Healthcare Revenue Cycle Management (RCM) Analytics Dashboard
A comprehensive Streamlit application for monitoring RCM KPIs.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.data_loader import load_all_data
from src.metrics import (
    calc_days_in_ar,
    calc_net_collection_rate,
    calc_gross_collection_rate,
    calc_clean_claim_rate,
    calc_denial_rate,
    calc_denial_reasons,
    calc_first_pass_rate,
    calc_charge_lag,
    calc_cost_to_collect,
    calc_ar_aging,
    calc_payment_accuracy,
    calc_bad_debt_rate,
    calc_appeal_success_rate,
    calc_avg_reimbursement,
    calc_payer_mix,
    calc_denial_rate_by_payer,
    calc_department_performance,
)

# ── Page Config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Healthcare RCM Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────────────
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


# ── Load Data ────────────────────────────────────────────────────────
@st.cache_data
def get_data():
    return load_all_data()


data = get_data()
claims = data["claims"]
payments = data["payments"]
denials = data["denials"]
adjustments = data["adjustments"]
encounters = data["encounters"]
charges = data["charges"]
payers = data["payers"]
operating_costs = data["operating_costs"]

# ── Sidebar Filters ─────────────────────────────────────────────────
st.sidebar.title("Filters")

# Date range filter
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

# Filter related tables
claim_ids = f_claims["claim_id"].unique()
f_payments = payments[payments["claim_id"].isin(claim_ids)].copy()
f_denials = denials[denials["claim_id"].isin(claim_ids)].copy()
f_adjustments = adjustments[adjustments["claim_id"].isin(claim_ids)].copy()
f_charges = charges[charges["encounter_id"].isin(f_encounters["encounter_id"].unique())].copy()

# ── Header ───────────────────────────────────────────────────────────
st.title("Healthcare RCM Analytics Dashboard")
st.caption(f"Analyzing {len(f_claims):,} claims | {len(f_encounters):,} encounters | Date range: {start_dt.strftime('%b %Y')} to {end_dt.strftime('%b %Y')}")

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
with tab1:
    st.header("Executive Summary")

    dar_val, dar_trend = calc_days_in_ar(f_claims, f_payments)
    ncr_val, ncr_trend = calc_net_collection_rate(f_claims, f_payments, f_adjustments)
    gcr_val, gcr_trend = calc_gross_collection_rate(f_claims, f_payments)
    ccr_val, ccr_trend = calc_clean_claim_rate(f_claims)
    denial_val, denial_trend = calc_denial_rate(f_claims)
    fpr_val, fpr_trend = calc_first_pass_rate(f_claims)
    accuracy_val = calc_payment_accuracy(f_payments)
    bad_debt_val, bad_debt_amt, total_charges = calc_bad_debt_rate(f_claims, f_adjustments)

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
with tab2:
    st.header("Collections & Revenue Analysis")

    gcr_val, gcr_trend = calc_gross_collection_rate(f_claims, f_payments)
    ncr_val, ncr_trend = calc_net_collection_rate(f_claims, f_payments, f_adjustments)
    ctc_val, ctc_trend = calc_cost_to_collect(operating_costs, f_claims, f_payments)
    avg_reimb, reimb_trend = calc_avg_reimbursement(f_claims, f_payments)
    bad_debt_val, bad_debt_amt, total_charges_val = calc_bad_debt_rate(f_claims, f_adjustments)

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
    st.dataframe(pd.DataFrame(fin_data), hide_index=True, use_container_width=True)


# =====================================================================
# TAB 3: CLAIMS & DENIALS
# =====================================================================
with tab3:
    st.header("Claims & Denials Analysis")

    ccr_val, ccr_trend = calc_clean_claim_rate(f_claims)
    denial_val, denial_trend = calc_denial_rate(f_claims)
    fpr_val, fpr_trend = calc_first_pass_rate(f_claims)
    denial_reasons = calc_denial_reasons(f_denials)
    charge_lag_val, charge_lag_trend, charge_lag_dist = calc_charge_lag(f_charges)
    appeal_rate, total_appealed, won_appeals = calc_appeal_success_rate(f_denials)

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
        st.dataframe(
            denial_reasons[["denial_reason_code", "denial_reason_description", "count",
                            "total_denied_amount", "total_recovered", "recovery_rate"]].round(2),
            hide_index=True, use_container_width=True,
        )


# =====================================================================
# TAB 4: A/R AGING & CASH FLOW
# =====================================================================
with tab4:
    st.header("Accounts Receivable Aging & Cash Flow")

    dar_val, dar_trend = calc_days_in_ar(f_claims, f_payments)
    aging_summary, total_ar = calc_ar_aging(f_claims, f_payments)

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


# =====================================================================
# TAB 5: PAYER ANALYSIS
# =====================================================================
with tab5:
    st.header("Payer Performance Analysis")

    payer_mix = calc_payer_mix(f_claims, f_payments, payers)
    denial_by_payer = calc_denial_rate_by_payer(f_claims, payers)

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


# =====================================================================
# TAB 6: DEPARTMENT PERFORMANCE
# =====================================================================
with tab6:
    st.header("Department Performance")

    dept_perf = calc_department_performance(f_encounters, f_claims, f_payments)

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


# ── Footer ───────────────────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.markdown("### Data Summary")
st.sidebar.markdown(f"- **Patients:** {len(data['patients']):,}")
st.sidebar.markdown(f"- **Providers:** {len(data['providers']):,}")
st.sidebar.markdown(f"- **Encounters:** {len(f_encounters):,}")
st.sidebar.markdown(f"- **Claims:** {len(f_claims):,}")
st.sidebar.markdown(f"- **Payments:** {len(f_payments):,}")
st.sidebar.markdown(f"- **Denials:** {len(f_denials):,}")

st.sidebar.divider()
st.sidebar.caption("Healthcare RCM Analytics Dashboard v1.0")
