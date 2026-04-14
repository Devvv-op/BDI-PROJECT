from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


DATA_PATH = Path("data/processed/final_output.csv")

RISK_COLORS = {
    "Low": "#2ca25f",
    "Medium": "#f59e0b",
    "High": "#dc2626",
    "Unknown": "#6b7280",
}

ANOMALY_COLORS = {
    "Normal": "#2ca25f",
    "Z-score": "#f59e0b",
    "IQR": "#ef4444",
    "Both": "#7f1d1d",
}


st.set_page_config(
    page_title="PDS Leakage Risk Dashboard",
    page_icon="",
    layout="wide",
)


@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    """Load the project final output dataset."""
    df = pd.read_csv(path)

    if "risk_level" in df.columns:
        df["risk_level"] = df["risk_level"].fillna("Unknown").astype(str)

    for column in df.select_dtypes(include=["bool"]).columns:
        df[column] = df[column].astype(bool)

    return df


def format_number(value: float) -> str:
    return f"{value:,.2f}"


def get_filtered_data(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    districts = sorted(df["district"].dropna().astype(str).unique())
    selected_districts = st.sidebar.multiselect(
        "District",
        options=districts,
        default=districts,
    )

    risk_order = ["Low", "Medium", "High", "Unknown"]
    risk_options = [risk for risk in risk_order if risk in set(df["risk_level"])]
    selected_risks = st.sidebar.multiselect(
        "Risk level",
        options=risk_options,
        default=risk_options,
    )

    search_text = st.sidebar.text_input(
        "Search shop or district",
        placeholder="Type shop ID, shop name, district...",
    ).strip()

    filtered = df[
        df["district"].astype(str).isin(selected_districts)
        & df["risk_level"].isin(selected_risks)
    ].copy()

    if search_text:
        searchable_columns = [
            column
            for column in ["district", "shop_id", "shop_name", "month", "risk_level"]
            if column in filtered.columns
        ]
        search_mask = pd.Series(False, index=filtered.index)
        for column in searchable_columns:
            search_mask |= filtered[column].astype(str).str.contains(
                search_text,
                case=False,
                na=False,
            )
        filtered = filtered[search_mask].copy()

    return filtered


def show_kpis(df: pd.DataFrame) -> None:
    total_shops = df["shop_id"].nunique() if "shop_id" in df else len(df)
    average_risk_score = df["risk_score"].mean() if "risk_score" in df else 0
    high_risk_percent = 0

    if {"shop_id", "risk_level"}.issubset(df.columns) and total_shops > 0:
        high_risk_shops = df.loc[df["risk_level"].eq("High"), "shop_id"].nunique()
        high_risk_percent = high_risk_shops / total_shops * 100
    elif "risk_level" in df.columns and len(df) > 0:
        high_risk_percent = df["risk_level"].eq("High").mean() * 100

    col1, col2, col3 = st.columns(3)
    col1.metric("Total shops", f"{total_shops:,}")
    col2.metric("Average risk score", format_number(average_risk_score))
    col3.metric("High-risk shops", f"{high_risk_percent:.1f}%")


def show_data_table(df: pd.DataFrame) -> None:
    st.subheader("Filtered Data")

    display_columns = [
        "district",
        "shop_id",
        "shop_name",
        "month",
        "allocation",
        "distribution",
        "utilization_ratio",
        "excess_distribution",
        "variance",
        "risk_score",
        "risk_level",
    ]
    display_columns = [column for column in display_columns if column in df.columns]

    st.dataframe(
        df[display_columns].sort_values("risk_score", ascending=False)
        if "risk_score" in display_columns
        else df[display_columns],
        use_container_width=True,
        hide_index=True,
    )


def allocation_distribution_chart(df: pd.DataFrame) -> go.Figure:
    by_district = (
        df.groupby("district", as_index=False)[["allocation", "distribution"]]
        .sum()
        .sort_values("distribution", ascending=False)
        .head(15)
    )
    melted = by_district.melt(
        id_vars="district",
        value_vars=["allocation", "distribution"],
        var_name="Supply type",
        value_name="Quantity",
    )
    fig = px.bar(
        melted,
        x="district",
        y="Quantity",
        color="Supply type",
        barmode="group",
        title="Allocation vs Distribution by District",
        labels={"district": "District"},
        color_discrete_map={
            "allocation": "#2563eb",
            "distribution": "#16a34a",
        },
    )
    fig.update_layout(xaxis_tickangle=-35)
    return fig


def utilization_histogram(df: pd.DataFrame) -> go.Figure:
    fig = px.histogram(
        df,
        x="utilization_ratio",
        nbins=40,
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        title="Utilization Ratio Distribution",
        labels={"utilization_ratio": "Utilization ratio"},
    )
    fig.update_layout(bargap=0.05)
    return fig


def top_shops_chart(df: pd.DataFrame) -> go.Figure:
    shop_label = "shop_id"
    if "shop_name" in df.columns:
        df = df.copy()
        df["shop_label"] = df["shop_id"].astype(str) + " - " + df["shop_name"].astype(str)
        shop_label = "shop_label"

    top_shops = (
        df.groupby(shop_label, as_index=False)["distribution"]
        .sum()
        .sort_values("distribution", ascending=False)
        .head(10)
    )
    fig = px.bar(
        top_shops,
        x="distribution",
        y=shop_label,
        orientation="h",
        title="Top 10 Shops by Distribution",
        labels={shop_label: "Shop", "distribution": "Total distribution"},
        color="distribution",
        color_continuous_scale="Greens",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    return fig


def top_districts_chart(df: pd.DataFrame) -> go.Figure:
    top_districts = (
        df.groupby("district", as_index=False)["distribution"]
        .sum()
        .sort_values("distribution", ascending=False)
        .head(10)
    )
    fig = px.bar(
        top_districts,
        x="district",
        y="distribution",
        title="Top Districts by Distribution",
        labels={"district": "District", "distribution": "Total distribution"},
        color="distribution",
        color_continuous_scale="Greens",
    )
    fig.update_layout(xaxis_tickangle=-35, coloraxis_showscale=False)
    return fig


def correlation_heatmap(df: pd.DataFrame) -> go.Figure:
    numeric_columns = [
        "allocation",
        "distribution",
        "beneficiaries",
        "utilization_ratio",
        "per_capita",
        "excess_distribution",
        "normalized_distribution",
        "variance",
        "risk_score",
    ]
    numeric_columns = [column for column in numeric_columns if column in df.columns]
    corr = df[numeric_columns].corr()

    fig = px.imshow(
        corr,
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        title="Correlation Matrix",
    )
    fig.update_layout(height=650)
    return fig


def add_anomaly_type(df: pd.DataFrame, base_column: str = "distribution") -> pd.DataFrame:
    z_col = f"{base_column}_anomaly_zscore"
    iqr_col = f"{base_column}_anomaly_iqr"

    result = df.copy()
    z_flag = result[z_col] if z_col in result.columns else False
    iqr_flag = result[iqr_col] if iqr_col in result.columns else False

    result[f"{base_column}_anomaly_type"] = "Normal"
    result.loc[z_flag & ~iqr_flag, f"{base_column}_anomaly_type"] = "Z-score"
    result.loc[~z_flag & iqr_flag, f"{base_column}_anomaly_type"] = "IQR"
    result.loc[z_flag & iqr_flag, f"{base_column}_anomaly_type"] = "Both"
    return result


def zscore_anomaly_chart(df: pd.DataFrame) -> go.Figure:
    df = add_anomaly_type(df, "distribution")
    fig = px.scatter(
        df,
        x="distribution_zscore",
        y="distribution",
        color="distribution_anomaly_type",
        color_discrete_map=ANOMALY_COLORS,
        hover_data=["district", "shop_id", "shop_name", "allocation", "risk_level"],
        title="Z-score Anomaly Detection",
        labels={
            "distribution_zscore": "Distribution z-score",
            "distribution": "Distribution",
            "distribution_anomaly_type": "Anomaly type",
        },
    )
    fig.add_vline(x=2, line_dash="dash", line_color="#dc2626")
    fig.add_vline(x=-2, line_dash="dash", line_color="#dc2626")
    return fig


def iqr_boxplot(df: pd.DataFrame) -> go.Figure:
    fig = px.box(
        df,
        x="risk_level",
        y="distribution",
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        points="outliers",
        title="IQR Boxplot for Distribution Outliers",
        labels={"risk_level": "Risk level", "distribution": "Distribution"},
    )
    return fig


def combined_anomaly_chart(df: pd.DataFrame) -> go.Figure:
    df = add_anomaly_type(df, "distribution").reset_index(names="record_index")
    fig = px.scatter(
        df,
        x="record_index",
        y="distribution",
        color="distribution_anomaly_type",
        color_discrete_map=ANOMALY_COLORS,
        hover_data=[
            "district",
            "shop_id",
            "shop_name",
            "allocation",
            "distribution_zscore",
            "risk_score",
            "risk_level",
        ],
        title="Combined Anomaly Detection",
        labels={
            "record_index": "Record index",
            "distribution": "Distribution",
            "distribution_anomaly_type": "Anomaly type",
        },
    )
    return fig


def risk_score_distribution(df: pd.DataFrame) -> go.Figure:
    fig = px.histogram(
        df,
        x="risk_score",
        nbins=40,
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        title="Risk Score Distribution",
        labels={"risk_score": "Risk score", "risk_level": "Risk level"},
    )
    fig.update_layout(bargap=0.05)
    return fig


def risk_level_bar(df: pd.DataFrame) -> go.Figure:
    counts = (
        df["risk_level"]
        .value_counts()
        .rename_axis("risk_level")
        .reset_index(name="shops")
    )
    fig = px.bar(
        counts,
        x="risk_level",
        y="shops",
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        title="Shops by Risk Level",
        labels={"risk_level": "Risk level", "shops": "Records"},
    )
    return fig


def main() -> None:
    st.title("PDS Leakage Risk Dashboard")
    st.caption("Interactive view of distribution patterns, anomalies, and risk levels.")

    if not DATA_PATH.exists():
        st.error(f"Dataset not found: {DATA_PATH}")
        st.stop()

    df = load_data(DATA_PATH)
    filtered = get_filtered_data(df)

    if filtered.empty:
        st.warning("No records match the selected filters.")
        st.stop()

    show_kpis(filtered)

    overview_tab, distribution_tab, anomaly_tab, risk_tab = st.tabs(
        [
            "Overview",
            "Distribution Analysis",
            "Anomaly Detection",
            "Risk Analysis",
        ]
    )

    with overview_tab:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(risk_level_bar(filtered), use_container_width=True)
        with col2:
            st.plotly_chart(utilization_histogram(filtered), use_container_width=True)
        show_data_table(filtered)

    with distribution_tab:
        st.plotly_chart(allocation_distribution_chart(filtered), use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(top_shops_chart(filtered), use_container_width=True)
        with col2:
            st.plotly_chart(top_districts_chart(filtered), use_container_width=True)
        st.plotly_chart(correlation_heatmap(filtered), use_container_width=True)

    with anomaly_tab:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(zscore_anomaly_chart(filtered), use_container_width=True)
        with col2:
            st.plotly_chart(iqr_boxplot(filtered), use_container_width=True)
        st.plotly_chart(combined_anomaly_chart(filtered), use_container_width=True)

    with risk_tab:
        st.plotly_chart(risk_score_distribution(filtered), use_container_width=True)
        show_data_table(filtered)


if __name__ == "__main__":
    main()
