import warnings
warnings.filterwarnings("ignore")

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TPC-H Sales Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 TPC-H Sales Dashboard")
st.caption("Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1")

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return get_active_session()

@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    session = get_connection()
    return session.sql(sql).to_pandas()

# ── Sidebar: date range filter ────────────────────────────────────────────────
st.sidebar.header("Filters")

min_date = date(1992, 1, 1)
max_date = date(1998, 8, 2)

start_date, end_date = st.sidebar.date_input(
    "Order date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

status_labels = {"F": "F — Fulfilled", "O": "O — Open", "P": "P — Pending"}
selected_statuses = st.sidebar.multiselect(
    "Order status",
    options=list(status_labels.keys()),
    default=list(status_labels.keys()),
    format_func=lambda x: status_labels[x],
)

if not selected_statuses:
    st.sidebar.error("Select at least one order status.")
    st.stop()

if isinstance(start_date, date) and isinstance(end_date, date) and start_date <= end_date:
    status_list = ", ".join(f"'{s}'" for s in selected_statuses)
    date_filter = f"O_ORDERDATE BETWEEN '{start_date}' AND '{end_date}' AND O_ORDERSTATUS IN ({status_list})"
else:
    st.sidebar.error("Select a valid date range.")
    st.stop()

# ── Query 1: Summary metrics ──────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_summary(date_filter: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            COUNT(*)            AS total_orders,
            SUM(O_TOTALPRICE)   AS total_revenue,
            AVG(O_TOTALPRICE)   AS avg_order_value
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        WHERE {date_filter}
    """)

# ── Query 2: Revenue by month ─────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_revenue_by_month(date_filter: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            DATE_TRUNC('month', O_ORDERDATE)  AS order_month,
            SUM(O_TOTALPRICE)                  AS revenue
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        WHERE {date_filter}
        GROUP BY 1
        ORDER BY 1
    """)

# ── Query 3: Orders by market segment ────────────────────────────────────────
@st.cache_data(ttl=600)
def load_market_segments(date_filter: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            C.C_MKTSEGMENT      AS market_segment,
            COUNT(*)            AS order_count
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS  O
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
          ON O.O_CUSTKEY = C.C_CUSTKEY
        WHERE {date_filter}
        GROUP BY 1
        ORDER BY 2 DESC
    """)

# ── Query 4: Order volume by day of week and month ───────────────────────────
@st.cache_data(ttl=600)
def load_heatmap(date_filter: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            DAYOFWEEK(O_ORDERDATE)  AS day_of_week,
            MONTH(O_ORDERDATE)      AS month,
            COUNT(*)                AS order_count
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        WHERE {date_filter}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

# ── Query 5: Top 10 customers ─────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_top_customers(date_filter: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            C.C_NAME          AS customer,
            SUM(O.O_TOTALPRICE) AS total_order_value
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS  O
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
          ON O.O_CUSTKEY = C.C_CUSTKEY
        WHERE {date_filter}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading data from Snowflake…"):
    summary_df    = load_summary(date_filter)
    monthly_df    = load_revenue_by_month(date_filter)
    segments_df   = load_market_segments(date_filter)
    heatmap_df    = load_heatmap(date_filter)
    customers_df  = load_top_customers(date_filter)

# ── Metric row ────────────────────────────────────────────────────────────────
total_orders  = int(summary_df["TOTAL_ORDERS"].iloc[0])
total_revenue = float(summary_df["TOTAL_REVENUE"].iloc[0])
avg_order     = float(summary_df["AVG_ORDER_VALUE"].iloc[0])

col1, col2, col3 = st.columns(3)
col1.metric("Total Orders",       f"{total_orders:,}")
col2.metric("Total Revenue",      f"${total_revenue:,.0f}")
col3.metric("Avg Order Value",    f"${avg_order:,.2f}")

st.divider()

# ── Charts row ────────────────────────────────────────────────────────────────
left, right = st.columns(2)

# Line chart — revenue by month
with left:
    st.subheader("Total Revenue by Month")
    fig_line = px.line(
        monthly_df,
        x="ORDER_MONTH",
        y="REVENUE",
        labels={"ORDER_MONTH": "Month", "REVENUE": "Revenue (USD)"},
        template="plotly_white",
    )
    fig_line.update_traces(line_color="#1f77b4", line_width=2)
    fig_line.update_layout(
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_line, use_container_width=True)

# Bar chart — top 10 customers
with right:
    st.subheader("Top 10 Customers by Order Value")
    fig_bar = px.bar(
        customers_df.sort_values("TOTAL_ORDER_VALUE"),
        x="TOTAL_ORDER_VALUE",
        y="CUSTOMER",
        orientation="h",
        labels={"TOTAL_ORDER_VALUE": "Total Order Value (USD)", "CUSTOMER": ""},
        template="plotly_white",
        color="TOTAL_ORDER_VALUE",
        color_continuous_scale="Blues",
    )
    fig_bar.update_layout(
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        coloraxis_showscale=False,
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# Heatmap — order volume by day of week × month
st.subheader("Order Volume by Day of Week × Month")

DAY_NAMES   = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
MONTH_NAMES = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
               7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

heatmap_df["DAY_LABEL"]   = heatmap_df["DAY_OF_WEEK"].map(DAY_NAMES)
heatmap_df["MONTH_LABEL"] = heatmap_df["MONTH"].map(MONTH_NAMES)

pivot = (
    heatmap_df
    .pivot(index="DAY_LABEL", columns="MONTH_LABEL", values="ORDER_COUNT")
    .reindex(index=list(DAY_NAMES.values()),
             columns=list(MONTH_NAMES.values()))
)

fig_heat = go.Figure(go.Heatmap(
    z=pivot.values,
    x=pivot.columns.tolist(),
    y=pivot.index.tolist(),
    colorscale="Blues",
    hovertemplate="<b>%{y}, %{x}</b><br>Orders: %{z:,}<extra></extra>",
))
fig_heat.update_layout(
    template="plotly_white",
    xaxis_title="Month",
    yaxis_title="Day of Week",
    margin=dict(t=20, b=20),
)
st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# Pie chart — orders by market segment
st.subheader("Orders by Market Segment")
fig_pie = px.pie(
    segments_df,
    names="MARKET_SEGMENT",
    values="ORDER_COUNT",
    template="plotly_white",
    color_discrete_sequence=px.colors.qualitative.Pastel,
)
fig_pie.update_traces(textposition="inside", textinfo="percent+label")
fig_pie.update_layout(margin=dict(t=20, b=20), showlegend=True)
st.plotly_chart(fig_pie, use_container_width=True)
