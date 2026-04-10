import warnings
warnings.filterwarnings("ignore")

import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Municipal Financial Dashboard",
    page_icon="🏛️",
    layout="wide",
)

# ── Colour palette ────────────────────────────────────────────────────────────
BLUE  = "#4E7FA6"
TEAL  = "#2A9D8F"
LBLUE = "#89B4CC"
CATEGORY_COLORS = {"TAXES": BLUE, "GRANTS": TEAL, "FEES": LBLUE}

# ── Data queries ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_year_range() -> tuple[int, int]:
    session = get_active_session()
    df = session.sql("""
        SELECT MIN(YEAR(date)) AS min_year, MAX(YEAR(date)) AS max_year
        FROM CLARITY_DB.PUBLIC.MUNICIPAL_REVENUE
    """).to_pandas()
    return int(df["MIN_YEAR"].iloc[0]), int(df["MAX_YEAR"].iloc[0])

@st.cache_data(ttl=600)
def load_kpis(last_year: int) -> tuple[float, int, float]:
    session = get_active_session()
    rev = session.sql(f"""
        SELECT SUM(revenue) AS total_revenue
        FROM CLARITY_DB.PUBLIC.MUNICIPAL_REVENUE
        WHERE YEAR(date) = {last_year}
    """).to_pandas()["TOTAL_REVENUE"].iloc[0]

    permits = session.sql(f"""
        SELECT
            SUM(number_of_permits) AS total_permits,
            SUM(project_value)     AS total_project_value
        FROM CLARITY_DB.PUBLIC.BUILDING_PERMITS
        WHERE YEAR(date) = {last_year}
    """).to_pandas()

    return float(rev), int(permits["TOTAL_PERMITS"].iloc[0]), float(permits["TOTAL_PROJECT_VALUE"].iloc[0])

@st.cache_data(ttl=600)
def load_revenue_pie(year: int):
    session = get_active_session()
    return session.sql(f"""
        SELECT category, SUM(revenue) AS revenue
        FROM CLARITY_DB.PUBLIC.MUNICIPAL_REVENUE
        WHERE YEAR(date) = {year}
        GROUP BY 1
    """).to_pandas()

@st.cache_data(ttl=600)
def load_revenue_trend(start_year: int, end_year: int):
    session = get_active_session()
    return session.sql(f"""
        SELECT
            DATE_TRUNC('month', date) AS month,
            category,
            SUM(revenue) AS revenue
        FROM CLARITY_DB.PUBLIC.MUNICIPAL_REVENUE
        WHERE YEAR(date) BETWEEN {start_year} AND {end_year}
        GROUP BY 1, 2
        ORDER BY 1
    """).to_pandas()

@st.cache_data(ttl=600)
def load_permits(start_year: int, end_year: int):
    session = get_active_session()
    return session.sql(f"""
        SELECT
            DATE_TRUNC('month', date) AS month,
            SUM(number_of_permits)    AS number_of_permits,
            SUM(project_value)        AS project_value
        FROM CLARITY_DB.PUBLIC.BUILDING_PERMITS
        WHERE YEAR(date) BETWEEN {start_year} AND {end_year}
        GROUP BY 1
        ORDER BY 1
    """).to_pandas()

# ── Sidebar ───────────────────────────────────────────────────────────────────
min_year, max_year = load_year_range()
last_year = max_year - 1  # e.g. 2025 when max is 2026

st.sidebar.header("Filters")
start_year, end_year = st.sidebar.slider(
    "Year range",
    min_value=min_year,
    max_value=max_year,
    value=(min_year, max_year),
)

# ── Header & KPIs ─────────────────────────────────────────────────────────────
st.title("🏛️ Municipal Financial Dashboard")
st.caption("Source: CLARITY_DB.PUBLIC")

total_revenue, total_permits, total_project_value = load_kpis(last_year)

k1, k2, k3 = st.columns(3)
k1.metric(f"Total Revenue ({last_year})",       f"${total_revenue:,.0f}")
k2.metric(f"Total Permits ({last_year})",        f"{total_permits:,}")
k3.metric(f"Total Project Value ({last_year})",  f"${total_project_value:,.0f}")

st.divider()

# ── Load filtered data ────────────────────────────────────────────────────────
with st.spinner("Loading data…"):
    revenue_df = load_revenue_trend(start_year, end_year)
    permits_df = load_permits(start_year, end_year)

# ── Row 1: Revenue pie  |  Revenue trend ─────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"{last_year} Revenue by Category")
    pie_df = load_revenue_pie(last_year)
    fig_pie = px.pie(
        pie_df,
        names="CATEGORY",
        values="REVENUE",
        color="CATEGORY",
        color_discrete_map=CATEGORY_COLORS,
        template="plotly_white",
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
    fig_pie.update_layout(showlegend=True, margin=dict(t=20, b=20))
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("Revenue Trend by Category (2021–2025)")
    fig_line = px.line(
        revenue_df,
        x="MONTH",
        y="REVENUE",
        color="CATEGORY",
        color_discrete_map=CATEGORY_COLORS,
        labels={"MONTH": "Month", "REVENUE": "Revenue (USD)", "CATEGORY": "Category"},
        template="plotly_white",
    )
    fig_line.update_layout(
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        margin=dict(t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# ── Row 2: Combo chart  |  Avg permit value ───────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Building Permits — Volume and Value")
    fig_combo = make_subplots(specs=[[{"secondary_y": True}]])

    fig_combo.add_trace(
        go.Bar(
            x=permits_df["MONTH"],
            y=permits_df["NUMBER_OF_PERMITS"],
            name="Number of Permits",
            marker_color=BLUE,
            opacity=0.8,
        ),
        secondary_y=False,
    )
    fig_combo.add_trace(
        go.Scatter(
            x=permits_df["MONTH"],
            y=permits_df["PROJECT_VALUE"],
            name="Project Value",
            mode="lines",
            line=dict(color=TEAL, width=2),
        ),
        secondary_y=True,
    )

    fig_combo.update_xaxes(title_text="Month")
    fig_combo.update_yaxes(title_text="Number of Permits", secondary_y=False)
    fig_combo.update_yaxes(
        title_text="Project Value (USD)",
        tickprefix="$", tickformat=",.0f",
        secondary_y=True,
    )
    fig_combo.update_layout(
        template="plotly_white",
        margin=dict(t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_combo, use_container_width=True)

with col4:
    st.subheader("Average Project Value per Permit Over Time")
    avg_df = permits_df.copy()
    avg_df["AVG_VALUE"] = avg_df["PROJECT_VALUE"] / avg_df["NUMBER_OF_PERMITS"]

    fig_avg = px.line(
        avg_df,
        x="MONTH",
        y="AVG_VALUE",
        labels={"MONTH": "Month", "AVG_VALUE": "Avg Value per Permit (USD)"},
        template="plotly_white",
    )
    fig_avg.update_traces(line_color=TEAL, line_width=2)
    fig_avg.update_layout(
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_avg, use_container_width=True)
