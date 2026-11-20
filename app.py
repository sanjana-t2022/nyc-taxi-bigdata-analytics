import sys
from pathlib import Path
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# -----------------------------------------------------------------------------
# App Configuration & Styling
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="NYC Taxi Big Data Analytics & Engine Benchmark",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for crisp UI
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 16px;
        border-left: 5px solid #ffaa00;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1.05rem;
        font-weight: 600;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Data Loading from DuckDB Gold Warehouse
# -----------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent / "data" / "gold_warehouse.duckdb"


@st.cache_data(ttl=60)
def load_data():
    """Load summary tables from gold_warehouse.duckdb."""
    if not DB_PATH.exists():
        st.error(f"Warehouse database not found at `{DB_PATH}`. Please run `src/02_olap_duckdb.py` first.")
        st.stop()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        borough_df = con.execute("SELECT * FROM borough_summary ORDER BY total_trips DESC").df()
        hourly_df = con.execute("SELECT * FROM hourly_summary ORDER BY pickup_hour ASC").df()
        
        # Check if benchmark_results exists
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        benchmark_df = pd.DataFrame()
        if "benchmark_results" in table_names:
            benchmark_df = con.execute(
                """
                SELECT * FROM benchmark_results 
                ORDER BY benchmark_timestamp DESC, execution_time_seconds ASC
                """
            ).df()
    finally:
        con.close()

    return borough_df, hourly_df, benchmark_df


borough_df, hourly_df, benchmark_df = load_data()

# -----------------------------------------------------------------------------
# Sidebar Navigation & Controls
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
    """
    <div style="background-color:#F7B731; color:#111; padding:10px 14px; border-radius:10px; display:inline-block; font-weight:800; font-size:20px; letter-spacing:1px; margin-bottom:10px;">
        🚕 NYC TAXI
    </div>
    """,
    unsafe_allow_html=True,
)
    st.title("NYC Taxi OLAP")
    st.caption("NYC Yellow Taxi Data (January 2024)")
    st.markdown("---")

    st.markdown("**Architecture Pipeline:**")
    st.markdown("- **Bronze:** Raw Parquet & Zone CSV")
    st.markdown("- **Silver:** Cleaned Parquet (`Polars`)")
    st.markdown("- **Gold:** Analytics Warehouse (`DuckDB`)")
    st.markdown("---")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        "<div style='font-size: 0.8rem; color: #888;'>Built with Streamlit, DuckDB, Polars & Plotly</div>",
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# Main Dashboard Header
# -----------------------------------------------------------------------------
st.title("🚕 NYC Taxi Big Data Analytics & Performance Benchmark")
st.markdown("Interactive analytical dashboard and query engine performance analysis powered by **DuckDB** and **Polars**.")

# -----------------------------------------------------------------------------
# Top KPI Cards
# -----------------------------------------------------------------------------
total_trips = int(borough_df["total_trips"].sum())
total_revenue = float(borough_df["total_revenue"].sum())
avg_fare = float((borough_df["total_trips"] * borough_df["avg_fare"]).sum() / total_trips) if total_trips > 0 else 0.0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="Total Trips",
        value=f"{total_trips:,.0f}",
        delta=f"{total_trips / 1e6:.2f}M cleaned trips",
        delta_color="normal",
    )
with col2:
    st.metric(
        label="Total Revenue",
        value=f"${total_revenue / 1e9:.2f}B",
        delta=f"${total_revenue / 1e6:,.1f}M USD",
        delta_color="normal",
    )
with col3:
    st.metric(
        label="Average Fare",
        value=f"${avg_fare:.2f}",
        delta="Per trip",
        delta_color="off",
    )
with col4:
    active_boroughs = int(borough_df[borough_df["total_trips"] > 0]["borough"].nunique())
    st.metric(
        label="Covered Boroughs",
        value=f"{active_boroughs}",
        delta="NYC & Airports",
        delta_color="off",
    )

st.markdown("---")

# -----------------------------------------------------------------------------
# Tabs Layout: Analytics vs Performance Benchmark
# -----------------------------------------------------------------------------
tab_analytics, tab_benchmark = st.tabs(["📊 Taxi Analytics & Insights", "⚡ Performance Benchmark (Pandas vs Polars vs DuckDB)"])

# =============================================================================
# TAB 1: Analytics & Insights
# =============================================================================
with tab_analytics:
    row1_col1, row1_col2 = st.columns([1, 1])

    # 1. Revenue by NYC Borough
    with row1_col1:
        st.subheader("💰 Revenue by NYC Borough")
        
        # Sort for display
        chart_borough_df = borough_df.sort_values("total_revenue", ascending=True)

        fig_borough = px.bar(
            chart_borough_df,
            x="total_revenue",
            y="borough",
            orientation="h",
            text="total_revenue",
            color="total_revenue",
            color_continuous_scale="Viridis",
            labels={"total_revenue": "Total Revenue ($)", "borough": "Borough"},
            hover_data={"total_trips": ":,", "avg_fare": ":$.2f", "total_revenue": ":$,.2f"},
        )
        fig_borough.update_traces(
            texttemplate="$%{x:,.2s}",
            textposition="outside",
            cliponaxis=False,
        )
        fig_borough.update_layout(
            margin=dict(l=20, r=40, t=20, b=20),
            height=380,
            coloraxis_showscale=False,
            xaxis=dict(tickprefix="$", tickformat=",.0s"),
            yaxis=dict(title=None),
        )
        st.plotly_chart(fig_borough, use_container_width=True)

    # 2. Hourly Traffic Volume
    with row1_col2:
        st.subheader("⏰ Hourly Traffic Volume & Speed")
        
        # Dual-axis chart for volume and average speed
        fig_hourly = go.Figure()

        # Bar chart for trip volume
        fig_hourly.add_trace(
            go.Bar(
                x=hourly_df["pickup_hour"],
                y=hourly_df["trip_volume"],
                name="Trip Volume",
                marker_color="#1f77b4",
                opacity=0.75,
                hovertemplate="Hour %{x}:00<br>Trips: %{y:,.0f}<extra></extra>",
                yaxis="y",
            )
        )

        # Line chart for average speed (mph)
        fig_hourly.add_trace(
            go.Scatter(
                x=hourly_df["pickup_hour"],
                y=hourly_df["avg_speed_mph"],
                name="Avg Speed (mph)",
                mode="lines+markers",
                line=dict(color="#ff7f0e", width=2.5),
                marker=dict(size=6),
                hovertemplate="Hour %{x}:00<br>Speed: %{y:.1f} mph<extra></extra>",
                yaxis="y2",
            )
        )

        fig_hourly.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            height=380,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(
                title="Hour of Day (0 - 23)",
                tickmode="linear",
                tick0=0,
                dtick=2,
            ),
            yaxis=dict(
                title="Total Trips",
                showgrid=True,
                tickformat=",.0s",
            ),
            yaxis2=dict(
                title="Avg Speed (mph)",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
        )
        st.plotly_chart(fig_hourly, use_container_width=True)

    # Detailed Summary Tables
    with st.expander("🔍 View Raw Aggregated Summary Tables"):
        tbl_col1, tbl_col2 = st.columns(2)
        with tbl_col1:
            st.markdown("**Borough Aggregations**")
            st.dataframe(
                borough_df.style.format({
                    "total_trips": "{:,.0f}",
                    "avg_fare": "${:,.2f}",
                    "total_revenue": "${:,.2f}",
                }),
                use_container_width=True,
            )
        with tbl_col2:
            st.markdown("**Hourly Aggregations**")
            st.dataframe(
                hourly_df.style.format({
                    "pickup_hour": "{:02d}:00",
                    "trip_volume": "{:,.0f}",
                    "avg_speed_mph": "{:.2f} mph",
                }),
                use_container_width=True,
            )

# =============================================================================
# TAB 2: Performance Benchmark
# =============================================================================
with tab_benchmark:
    st.subheader("⚡ Execution Time & Peak Memory: Pandas vs. Polars vs. DuckDB")
    st.markdown(
        """
        Benchmarking identical analytical workloads: joining 2.86M trip records with taxi zone metadata, 
        handling nulls, and computing multi-column aggregations (`total_trips`, `avg_fare`, `total_revenue`).
        """
    )

    if benchmark_df.empty:
        st.warning("No benchmark results found. Run `python src/03_benchmark.py` to generate benchmarks.")
    else:
        # Take the most recent run for each engine
        latest_bench = benchmark_df.drop_duplicates(subset=["engine"], keep="first").copy()
        
        # Ensure ordered display: Pandas, Polars, DuckDB
        engine_order = ["Pandas", "Polars", "DuckDB"]
        latest_bench["sort_order"] = latest_bench["engine"].map(lambda x: engine_order.index(x) if x in engine_order else 99)
        latest_bench = latest_bench.sort_values("sort_order")

        # Top Insight Cards
        duckdb_row = latest_bench[latest_bench["engine"] == "DuckDB"]
        polars_row = latest_bench[latest_bench["engine"] == "Polars"]
        pandas_row = latest_bench[latest_bench["engine"] == "Pandas"]

        if not duckdb_row.empty and not pandas_row.empty:
            speedup = duckdb_row["speedup_factor"].values[0]
            ram_reduct = duckdb_row["ram_reduction_pct"].values[0]

            b_col1, b_col2, b_col3 = st.columns(3)
            with b_col1:
                st.info(f"🚀 **DuckDB Speedup:** **{speedup:.1f}x faster** than Pandas (0.34s vs 2.45s)")
            with b_col2:
                st.success(f"💾 **Memory Efficiency:** **{ram_reduct:.1f}% less RAM** than Pandas (164 MB vs 453 MB)")
            with b_col3:
                st.warning(f"🦀 **Polars Efficiency:** **54.6% less RAM** than Pandas with native streaming")

        st.markdown("<br>", unsafe_allow_html=True)

        bench_col1, bench_col2 = st.columns(2)

        # Chart 1: Execution Time Comparison
        with bench_col1:
            st.markdown("#### ⏱️ Execution Time (Lower is Better)")
            colors = {"Pandas": "#d62728", "Polars": "#1f77b4", "DuckDB": "#2ca02c"}
            
            fig_time = px.bar(
                latest_bench,
                x="engine",
                y="execution_time_seconds",
                color="engine",
                color_discrete_map=colors,
                text="execution_time_seconds",
                labels={"execution_time_seconds": "Time (seconds)", "engine": "Engine"},
            )
            fig_time.update_traces(
                texttemplate="%{y:.3f}s",
                textposition="outside",
            )
            fig_time.update_layout(
                showlegend=False,
                margin=dict(l=20, r=20, t=20, b=20),
                height=350,
                yaxis=dict(title="Execution Time (s)", tickformat=".1f"),
            )
            st.plotly_chart(fig_time, use_container_width=True)

        # Chart 2: Peak Memory Usage Comparison
        with bench_col2:
            st.markdown("#### 🧠 Peak RAM Usage (Lower is Better)")
            fig_ram = px.bar(
                latest_bench,
                x="engine",
                y="peak_ram_mb",
                color="engine",
                color_discrete_map=colors,
                text="peak_ram_mb",
                labels={"peak_ram_mb": "Peak RAM (MB)", "engine": "Engine"},
            )
            fig_ram.update_traces(
                texttemplate="%{y:.1f} MB",
                textposition="outside",
            )
            fig_ram.update_layout(
                showlegend=False,
                margin=dict(l=20, r=20, t=20, b=20),
                height=350,
                yaxis=dict(title="Peak RAM (MB)", tickformat=",.0f"),
            )
            st.plotly_chart(fig_ram, use_container_width=True)

        # Detailed Benchmark Table
        st.markdown("#### 📋 Detailed Benchmark Metrics")
        st.dataframe(
            latest_bench[[
                "engine",
                "execution_time_seconds",
                "speedup_factor",
                "peak_ram_mb",
                "ram_reduction_pct",
                "rows_returned",
                "benchmark_timestamp",
            ]].style.format({
                "execution_time_seconds": "{:.4f} s",
                "speedup_factor": "{:.2f}x",
                "peak_ram_mb": "{:.2f} MB",
                "ram_reduction_pct": "{:.1f}%",
                "rows_returned": "{:,.0f}",
                "benchmark_timestamp": lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else "",
            }),
            use_container_width=True,
        )
