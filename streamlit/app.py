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
from datetime import datetime

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
    """Initialize BigQuery client — project ID always passed explicitly."""
    try:
        project_id = st.secrets.get("GCP_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")

        if "gcp_service_account" in st.secrets:
            # Streamlit Cloud — use service account from secrets
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(credentials=credentials, project=project_id)

        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            # Local dev with key file or GitHub Actions ADC
            return bigquery.Client(project=project_id)

        else:
            # Codespaces / local ADC
            return bigquery.Client(project=project_id)

    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {e}")
        return None


def get_project_id() -> str:
    return st.secrets.get("GCP_PROJECT_ID") or os.getenv("GCP_PROJECT_ID", "")


# ── Data Loading Functions ───────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_monthly_activity():
    client = get_bigquery_client()
    project = get_project_id()
    if not client or not project:
        return pd.DataFrame()

    query = f"""
        SELECT
            transaction_month_key,
            transaction_year,
            transaction_month,
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
        FROM `{project}.sec_marts.mrt_monthly_insider_activity`
        ORDER BY transaction_month_key DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading monthly activity: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_top_traders(limit: int = 50):
    client = get_bigquery_client()
    project = get_project_id()
    if not client or not project:
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
        FROM `{project}.sec_marts.mrt_top_insider_traders`
        WHERE rank_by_value <= {limit}
        ORDER BY buy_sell_flag, rank_by_value
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading top traders: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_company_sentiment():
    client = get_bigquery_client()
    project = get_project_id()
    if not client or not project:
        return pd.DataFrame()

    query = f"""
        SELECT *
        FROM `{project}.sec_marts.mrt_company_insider_sentiment`
        ORDER BY ABS(net_value_usd) DESC
        LIMIT 100
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading company sentiment: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_company_list():
    client = get_bigquery_client()
    project = get_project_id()
    if not client or not project:
        return pd.DataFrame()

    query = f"""
        SELECT DISTINCT issuer_ticker, issuer_name
        FROM `{project}.sec_marts.mrt_monthly_insider_activity`
        WHERE issuer_ticker IS NOT NULL
        ORDER BY issuer_ticker
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading company list: {e}")
        return pd.DataFrame()


# ── Load Data ────────────────────────────────────────────────────────────────
monthly_df = load_monthly_activity()
traders_df = load_top_traders()
sentiment_df = load_company_sentiment()
company_list = load_company_list()

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("📊 Filters")

selected_companies = st.sidebar.multiselect(
    "Select Companies",
    options=company_list["issuer_ticker"].tolist() if not company_list.empty else [],
    default=[],
    help="Leave empty to show all companies",
)

transaction_type = st.sidebar.radio(
    "Transaction Type", options=["All", "Buy", "Sell"], horizontal=True
)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ── Main Title ───────────────────────────────────────────────────────────────
st.title("📈 SEC Insider Trading Dashboard")
st.markdown(
    "### Real-time analysis of insider trading activity from SEC Form 4 filings"
)

# ── KPI Row ──────────────────────────────────────────────────────────────────
if not monthly_df.empty:
    col1, col2, col3, col4, col5 = st.columns(5)

    total_buy_value = monthly_df[monthly_df["buy_sell_flag"] == "Buy"][
        "total_value_usd"
    ].sum()
    total_sell_value = monthly_df[monthly_df["buy_sell_flag"] == "Sell"][
        "total_value_usd"
    ].sum()
    total_transactions = monthly_df["num_transactions"].sum()
    unique_insiders = monthly_df["num_unique_insiders"].max()
    net_sentiment = total_buy_value - total_sell_value

    with col1:
        st.metric("Total Buy Value", f"${total_buy_value:,.0f}")
    with col2:
        st.metric("Total Sell Value", f"${total_sell_value:,.0f}")
    with col3:
        st.metric(
            "Net Sentiment",
            f"${net_sentiment:,.0f}",
            delta="Bullish" if net_sentiment > 0 else "Bearish",
        )
    with col4:
        st.metric("Total Transactions", f"{total_transactions:,}")
    with col5:
        st.metric("Unique Insiders", f"{int(unique_insiders):,}")
else:
    st.warning(
        "No data available. Ensure the pipeline has run and BigQuery mart tables are populated."
    )

# ── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Monthly Trends", "🏆 Top Insiders", "🏢 Company Sentiment", "📋 Raw Data"]
)

# ── Tab 1: Monthly Trends ────────────────────────────────────────────────────
with tab1:
    st.header("Monthly Insider Trading Trends")

    if not monthly_df.empty:
        filtered_df = monthly_df.copy()
        if selected_companies:
            filtered_df = filtered_df[
                filtered_df["issuer_ticker"].isin(selected_companies)
            ]
        if transaction_type != "All":
            filtered_df = filtered_df[filtered_df["buy_sell_flag"] == transaction_type]

        col1, col2 = st.columns([2, 1])

        with col1:
            monthly_volume = (
                filtered_df.groupby("transaction_month_key")
                .agg({"total_value_usd": "sum", "num_transactions": "sum"})
                .reset_index()
                .sort_values("transaction_month_key")
            )
            fig = px.area(
                monthly_volume,
                x="transaction_month_key",
                y="total_value_usd",
                title="Monthly Transaction Value Over Time",
                labels={
                    "transaction_month_key": "Month",
                    "total_value_usd": "Total Value (USD)",
                },
                template="plotly_dark",
            )
            fig.update_traces(line_color="#00ff88", fillcolor="rgba(0, 255, 136, 0.2)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
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

        st.subheader("Net Insider Sentiment by Month")
        net_by_month = (
            filtered_df.groupby("transaction_month_key")["net_sentiment_usd"]
            .sum()
            .reset_index()
            .sort_values("transaction_month_key")
        )
        fig = px.bar(
            net_by_month,
            x="transaction_month_key",
            y="net_sentiment_usd",
            title="Net Insider Sentiment (Positive = Net Buying)",
            labels={
                "transaction_month_key": "Month",
                "net_sentiment_usd": "Net Sentiment (USD)",
            },
            template="plotly_dark",
        )
        fig.update_traces(
            marker_color=[
                "#00ff88" if v > 0 else "#ff4444"
                for v in net_by_month["net_sentiment_usd"]
            ]
        )
        st.plotly_chart(fig, use_container_width=True)

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

# ── Tab 2: Top Insiders ──────────────────────────────────────────────────────
with tab2:
    st.header("Top Insider Traders")

    if not traders_df.empty:
        col1, col2 = st.columns(2)

        with col1:
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

# ── Tab 3: Company Sentiment ─────────────────────────────────────────────────
with tab3:
    st.header("Company Insider Sentiment Analysis")

    if not sentiment_df.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            top_bullish = sentiment_df.nlargest(10, "net_value_usd")
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

            top_bearish = sentiment_df.nsmallest(10, "net_value_usd")
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
            st.subheader("Sentiment Distribution")
            sentiment_counts = sentiment_df["sentiment_label"].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Company Sentiment Breakdown",
                template="plotly_dark",
                color=sentiment_counts.index,
                color_discrete_map={
                    "Strong Buy Signal": "#00ff88",
                    "Moderate Buy Signal": "#88ff44",
                    "Moderate Sell Signal": "#ff8844",
                    "Strong Sell Signal": "#ff4444",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: Raw Data ───────────────────────────────────────────────────────────
with tab4:
    st.header("Raw Data Explorer")

    data_option = st.selectbox(
        "Select Dataset",
        options=["Monthly Activity", "Top Traders", "Company Sentiment"],
    )

    df_map = {
        "Monthly Activity": monthly_df,
        "Top Traders": traders_df,
        "Company Sentiment": sentiment_df,
    }
    filename_map = {
        "Monthly Activity": "monthly_insider_activity",
        "Top Traders": "top_traders",
        "Company Sentiment": "company_sentiment",
    }

    selected_df = df_map[data_option]
    if not selected_df.empty:
        st.dataframe(selected_df, use_container_width=True, height=500)
        st.download_button(
            label=f"Download {data_option} CSV",
            data=selected_df.to_csv(index=False),
            file_name=f"{filename_map[data_option]}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.info("No data available for this dataset.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>SEC Insider Trading Dashboard | Data sourced from SEC EDGAR | Updated monthly</p>
        <p>Built with Streamlit · BigQuery · dbt · Apache Spark</p>
    </div>
    """,
    unsafe_allow_html=True,
)
