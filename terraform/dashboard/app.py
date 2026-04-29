"""
SEC Insider Trading Dashboard
Connects to BigQuery mart tables for interactive analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime, timedelta

# ── Page Configuration ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC Insider Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── BigQuery Client Setup ───────────────────────────────────────────────────
@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client with credentials from environment or service account"""
    try:
        # Try using application default credentials (works in Cloud Run, local dev)
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            return bigquery.Client()
        # Or use service account from Streamlit secrets
        elif hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials)
        else:
            # Fallback for local development
            return bigquery.Client()
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {str(e)}")
        return None


# ── Data Loading Functions ───────────────────────────────────────────────────
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_monthly_activity():
    """Load monthly insider activity data"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()

    query = """
        SELECT 
            transaction_month_start,
            issuer_ticker,
            issuer_name,
            buy_sell_flag,
            num_filings,
            num_unique_insiders,
            num_transactions,
            total_shares,
            total_value_usd,
            avg_price_per_share,
            max_single_transaction_usd,
            net_sentiment_usd
        FROM `sec_marts.mrt_monthly_insider_activity`
        WHERE transaction_month_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 MONTH)
        ORDER BY transaction_month_start DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_top_traders(limit=50):
    """Load top insider traders data"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()

    query = f"""
        SELECT 
            insider_name,
            issuer_ticker,
            issuer_name,
            insider_role,
            officer_title,
            buy_sell_flag,
            num_transactions,
            num_active_months,
            total_shares,
            total_value_usd,
            avg_transaction_value_usd,
            max_transaction_value_usd,
            first_transaction_date,
            last_transaction_date,
            rank_by_value
        FROM `sec_marts.mrt_top_insider_traders`
        WHERE rank_by_value <= {limit}
        ORDER BY buy_sell_flag, rank_by_value
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_company_sentiment():
    """Load company sentiment data"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()

    query = """
        SELECT *
        FROM `sec_marts.mrt_company_insider_sentiment`
        ORDER BY ABS(net_value_usd) DESC
        LIMIT 100
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_company_list():
    """Load distinct list of companies for filters"""
    client = get_bigquery_client()
    if not client:
        return []

    query = """
        SELECT DISTINCT issuer_ticker, issuer_name
        FROM `sec_marts.mrt_monthly_insider_activity`
        ORDER BY issuer_ticker
    """
    df = client.query(query).to_dataframe()
    return df


# ── Load Data ───────────────────────────────────────────────────────────────
monthly_df = load_monthly_activity()
traders_df = load_top_traders()
sentiment_df = load_company_sentiment()
company_list = load_company_list()

# ── Sidebar Filters ─────────────────────────────────────────────────────────
st.sidebar.title("📊 Filters")

# Date Range Filter
if not monthly_df.empty:
    min_date = monthly_df["transaction_month_start"].min()
    max_date = monthly_df["transaction_month_start"].max()
    date_range = st.sidebar.date_input(
        "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )

# Company Filter
selected_companies = st.sidebar.multiselect(
    "Select Companies",
    options=company_list["issuer_ticker"].tolist() if not company_list.empty else [],
    default=[],
    help="Leave empty to show all companies",
)

# Transaction Type Filter
transaction_type = st.sidebar.radio(
    "Transaction Type", options=["All", "Buy", "Sell"], horizontal=True
)

# Refresh Button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ── Main Dashboard ──────────────────────────────────────────────────────────
st.title("📈 SEC Insider Trading Dashboard")
st.markdown(
    "### Real-time analysis of insider trading activity from SEC Form 4 filings"
)

# ── KPI Metrics Row ────────────────────────────────────────────────────────
if not monthly_df.empty:
    col1, col2, col3, col4, col5 = st.columns(5)

    # Filter data for KPIs
    total_buy_value = monthly_df[monthly_df["buy_sell_flag"] == "Buy"][
        "total_value_usd"
    ].sum()
    total_sell_value = monthly_df[monthly_df["buy_sell_flag"] == "Sell"][
        "total_value_usd"
    ].sum()
    total_transactions = monthly_df["num_transactions"].sum()
    unique_insiders = monthly_df["num_unique_insiders"].sum()
    net_sentiment = total_buy_value - total_sell_value

    with col1:
        st.metric("Total Buy Value", f"${total_buy_value:,.0f}", delta=None)

    with col2:
        st.metric("Total Sell Value", f"${total_sell_value:,.0f}", delta=None)

    with col3:
        st.metric(
            "Net Sentiment",
            f"${net_sentiment:,.0f}",
            delta="Bullish" if net_sentiment > 0 else "Bearish",
        )

    with col4:
        st.metric("Total Transactions", f"{total_transactions:,}")

    with col5:
        st.metric("Unique Insiders", f"{unique_insiders:,}")

else:
    st.warning(
        "No data available. Please ensure the BigQuery mart tables are populated."
    )

# ── Dashboard Tabs ──────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Monthly Trends", "🏆 Top Insiders", "🏢 Company Sentiment", "📋 Raw Data"]
)

# ── Tab 1: Monthly Trends ──────────────────────────────────────────────────
with tab1:
    st.header("Monthly Insider Trading Trends")

    if not monthly_df.empty:
        # Filter data
        filtered_df = monthly_df.copy()
        if selected_companies:
            filtered_df = filtered_df[
                filtered_df["issuer_ticker"].isin(selected_companies)
            ]
        if transaction_type != "All":
            filtered_df = filtered_df[filtered_df["buy_sell_flag"] == transaction_type]

        col1, col2 = st.columns([2, 1])

        with col1:
            # Monthly Volume Chart
            monthly_volume = (
                filtered_df.groupby("transaction_month_start")
                .agg({"total_value_usd": "sum", "num_transactions": "sum"})
                .reset_index()
            )

            fig = px.area(
                monthly_volume,
                x="transaction_month_start",
                y="total_value_usd",
                title="Monthly Transaction Value Over Time",
                labels={
                    "transaction_month_start": "Month",
                    "total_value_usd": "Total Value (USD)",
                },
                template="plotly_dark",
            )
            fig.update_traces(line_color="#00ff88", fillcolor="rgba(0, 255, 136, 0.2)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Buy vs Sell Distribution
            buy_sell_dist = (
                filtered_df.groupby("buy_sell_flag")["total_value_usd"]
                .sum()
                .reset_index()
            )

            fig = px.pie(
                buy_sell_dist,
                values="total_value_usd",
                names="buy_sell_flag",
                title="Buy vs Sell Distribution",
                color="buy_sell_flag",
                color_discrete_map={"Buy": "#00ff88", "Sell": "#ff4444"},
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Net Sentiment Chart
        st.subheader("Net Insider Sentiment by Month")
        net_sentiment = (
            filtered_df.groupby("transaction_month_start")["net_sentiment_usd"]
            .sum()
            .reset_index()
        )

        fig = px.bar(
            net_sentiment,
            x="transaction_month_start",
            y="net_sentiment_usd",
            title="Net Insider Sentiment (Positive = Net Buying)",
            labels={
                "transaction_month_start": "Month",
                "net_sentiment_usd": "Net Sentiment (USD)",
            },
            template="plotly_dark",
        )
        fig.update_traces(
            marker_color=net_sentiment["net_sentiment_usd"].apply(
                lambda x: "#00ff88" if x > 0 else "#ff4444"
            )
        )
        st.plotly_chart(fig, use_container_width=True)

        # Top Companies by Value
        st.subheader("Top Companies by Transaction Value")
        top_companies = (
            filtered_df.groupby("issuer_ticker")
            .agg(
                {
                    "total_value_usd": "sum",
                    "num_transactions": "sum",
                    "net_sentiment_usd": "sum",
                }
            )
            .nlargest(10, "total_value_usd")
            .reset_index()
        )

        fig = px.bar(
            top_companies,
            y="issuer_ticker",
            x="total_value_usd",
            title="Top 10 Companies by Total Transaction Value",
            labels={"issuer_ticker": "Company", "total_value_usd": "Total Value (USD)"},
            orientation="h",
            template="plotly_dark",
            color="net_sentiment_usd",
            color_continuous_scale=["#ff4444", "#ffff00", "#00ff88"],
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 2: Top Insiders ─────────────────────────────────────────────────────
with tab2:
    st.header("Top Insider Traders")

    if not traders_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Top Buyers
            st.subheader("🔝 Top 10 Buyers")
            top_buyers = traders_df[traders_df["buy_sell_flag"] == "Buy"].nlargest(
                10, "total_value_usd"
            )

            fig = px.bar(
                top_buyers,
                y="insider_name",
                x="total_value_usd",
                color="insider_role",
                title="Top Buyers by Transaction Value",
                labels={
                    "insider_name": "Insider",
                    "total_value_usd": "Total Value (USD)",
                    "insider_role": "Role",
                },
                orientation="h",
                template="plotly_dark",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top Sellers
            st.subheader("📉 Top 10 Sellers")
            top_sellers = traders_df[traders_df["buy_sell_flag"] == "Sell"].nlargest(
                10, "total_value_usd"
            )

            fig = px.bar(
                top_sellers,
                y="insider_name",
                x="total_value_usd",
                color="insider_role",
                title="Top Sellers by Transaction Value",
                labels={
                    "insider_name": "Insider",
                    "total_value_usd": "Total Value (USD)",
                    "insider_role": "Role",
                },
                orientation="h",
                template="plotly_dark",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Insider Role Distribution
        st.subheader("Insider Role Distribution")
        role_dist = (
            traders_df.groupby(["insider_role", "buy_sell_flag"])
            .size()
            .reset_index(name="count")
        )

        fig = px.sunburst(
            role_dist,
            path=["insider_role", "buy_sell_flag"],
            values="count",
            title="Insider Trading by Role and Type",
            template="plotly_dark",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Full Leaderboard
        st.subheader("Full Insider Leaderboard")
        st.dataframe(
            traders_df[
                [
                    "insider_name",
                    "issuer_ticker",
                    "insider_role",
                    "buy_sell_flag",
                    "total_value_usd",
                    "num_transactions",
                    "rank_by_value",
                ]
            ].style.format({"total_value_usd": "${:,.0f}"}),
            use_container_width=True,
            height=400,
        )

# ── Tab 3: Company Sentiment ────────────────────────────────────────────────
with tab3:
    st.header("Company Insider Sentiment Analysis")

    if not sentiment_df.empty:
        # Sentiment Gauge
        col1, col2 = st.columns([2, 1])

        with col1:
            # Top Companies by Sentiment
            top_bullish = sentiment_df.nlargest(10, "net_value_usd")
            top_bearish = sentiment_df.nsmallest(10, "net_value_usd")

            st.subheader("Most Bullish Companies (Net Insider Buying)")
            fig = px.bar(
                top_bullish,
                y="issuer_ticker",
                x="net_value_usd",
                title="Companies with Strongest Insider Buying",
                labels={"issuer_ticker": "Company", "net_value_usd": "Net Value (USD)"},
                orientation="h",
                template="plotly_dark",
                color="buy_ratio",
                color_continuous_scale=["#ffff00", "#00ff88"],
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Most Bearish Companies (Net Insider Selling)")
            fig = px.bar(
                top_bearish,
                y="issuer_ticker",
                x="net_value_usd",
                title="Companies with Strongest Insider Selling",
                labels={"issuer_ticker": "Company", "net_value_usd": "Net Value (USD)"},
                orientation="h",
                template="plotly_dark",
                color="buy_ratio",
                color_continuous_scale=["#ff4444", "#ffff00"],
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Sentiment Distribution
            st.subheader("Sentiment Distribution")
            sentiment_counts = sentiment_df["sentiment_label"].value_counts()

            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Company Sentiment Breakdown",
                template="plotly_dark",
                color=sentiment_counts.index,
                color_discrete_map={
                    "Strong_Buy": "#00ff88",
                    "Buy": "#88ff44",
                    "Sell": "#ff8844",
                    "Strong_Sell": "#ff4444",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: Raw Data ─────────────────────────────────────────────────────────
with tab4:
    st.header("Raw Data Explorer")

    data_option = st.selectbox(
        "Select Dataset",
        options=["Monthly Activity", "Top Traders", "Company Sentiment"],
    )

    if data_option == "Monthly Activity" and not monthly_df.empty:
        st.dataframe(monthly_df, use_container_width=True, height=500)

        # Download button
        csv = monthly_df.to_csv(index=False)
        st.download_button(
            label="Download Monthly Activity CSV",
            data=csv,
            file_name=f"monthly_insider_activity_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    elif data_option == "Top Traders" and not traders_df.empty:
        st.dataframe(traders_df, use_container_width=True, height=500)

        csv = traders_df.to_csv(index=False)
        st.download_button(
            label="Download Top Traders CSV",
            data=csv,
            file_name=f"top_traders_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    elif data_option == "Company Sentiment" and not sentiment_df.empty:
        st.dataframe(sentiment_df, use_container_width=True, height=500)

        csv = sentiment_df.to_csv(index=False)
        st.download_button(
            label="Download Company Sentiment CSV",
            data=csv,
            file_name=f"company_sentiment_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

# ── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>SEC Insider Trading Dashboard | Data sourced from SEC EDGAR | Updated monthly</p>
        <p>Built with Streamlit · BigQuery · dbt</p>
    </div>
    """,
    unsafe_allow_html=True,
)
