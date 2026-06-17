from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st

from db import fetch_table_data, get_missing_tables, load_database_config, ping_database

PAGE_TITLE = "IoT Log Intelligence Pipeline Analytics"
PREPARE_DATA_COMMAND = "docker compose run --build --rm dbt dbt run"
MART_TABLES = [
    "mart_device_risk_summary",
    "mart_attack_summary",
    "mart_protocol_metrics",
    "mart_pipeline_quality_summary",
]
TOP_N_OPTIONS = [5, 10, 15, 20, 25, 50]
RISK_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
RISK_DISPLAY_ORDER = ["HIGH", "MEDIUM", "LOW"]


@st.cache_data(ttl=30, show_spinner=False)
def load_table(table_name: str) -> pd.DataFrame:
    return fetch_table_data(table_name=table_name)


def render_section_intro(text: str) -> None:
    st.caption(text)


def render_empty_state(message: str, suggestion: str | None = None) -> None:
    st.info(message)
    if suggestion:
        st.caption(suggestion)


def render_query_warning(table_name: str, error_name: str) -> None:
    st.warning(
        f"The dashboard could not read `{table_name}` right now. Confirm PostgreSQL is available "
        "and that the dbt marts were built successfully."
    )
    st.caption(f"Dashboard error: {error_name}")


def render_connection_status() -> bool:
    config = load_database_config()

    try:
        ping_database()
    except psycopg.Error as error:
        st.error(
            "PostgreSQL is unavailable. Start the local services and verify the database "
            f"settings for host `{config.host}` and database `{config.database}`."
        )
        st.caption(f"Database error: {error.__class__.__name__}")
        return False

    st.success(
        f"Connected to PostgreSQL `{config.database}` on `{config.host}:{config.port}` "
        f"as `{config.user}`."
    )
    return True


def render_sidebar(
    device_df: pd.DataFrame, attack_df: pd.DataFrame, protocol_df: pd.DataFrame
) -> tuple[list[str], list[str], list[str], int]:
    st.sidebar.header("How To Use")
    st.sidebar.caption(
        "1. Start PostgreSQL and the pipeline services.\n"
        f"2. Build dbt marts with `{PREPARE_DATA_COMMAND}`.\n"
        "3. Refresh this page and explore the filters below."
    )
    st.sidebar.info("Filters apply to the Device Risk, Attack Summary, Protocol Metrics, and Raw Mart Tables sections.")

    st.sidebar.header("Filters")
    st.sidebar.caption("If the marts are empty, the dashboard will show friendly guidance instead of crashing.")

    device_risk_values = (
        device_df.get("risk_level", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()
    )
    risk_options = [level for level in RISK_DISPLAY_ORDER if level in device_risk_values]
    protocol_options = sorted(protocol_df.get("protocol", pd.Series(dtype=str)).dropna().astype(str).unique())
    attack_options = sorted(
        attack_df.get("attack_type", pd.Series(dtype=str)).dropna().astype(str).unique()
    )

    selected_risk_levels = st.sidebar.multiselect(
        "Risk level",
        options=risk_options,
        default=risk_options,
        help="Filter device risk charts and tables by risk level.",
    )
    selected_protocols = st.sidebar.multiselect(
        "Protocol",
        options=protocol_options,
        default=protocol_options,
        help="Filter protocol metrics by protocol name.",
    )
    selected_attack_types = st.sidebar.multiselect(
        "Attack type",
        options=attack_options,
        default=attack_options,
        help="Filter attack summary by attack type.",
    )
    top_n = st.sidebar.selectbox(
        "Top N",
        options=TOP_N_OPTIONS,
        index=1,
        help="Control how many rows appear in the ranked charts and tables.",
    )

    return selected_risk_levels, selected_attack_types, selected_protocols, top_n


def load_mart_frames(missing_tables: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    mart_frames: dict[str, pd.DataFrame] = {}
    mart_errors: dict[str, str] = {}

    for table_name in MART_TABLES:
        if table_name in missing_tables:
            mart_frames[table_name] = pd.DataFrame()
            continue

        try:
            mart_frames[table_name] = load_table(table_name)
        except psycopg.Error as error:
            mart_frames[table_name] = pd.DataFrame()
            mart_errors[table_name] = error.__class__.__name__
        except Exception as error:
            mart_frames[table_name] = pd.DataFrame()
            mart_errors[table_name] = error.__class__.__name__

    return mart_frames, mart_errors


def filter_by_values(dataframe: pd.DataFrame, column_name: str, selected_values: list[str]) -> pd.DataFrame:
    if dataframe.empty or column_name not in dataframe.columns:
        return dataframe
    if not selected_values:
        return dataframe.iloc[0:0].copy()
    return dataframe[dataframe[column_name].astype(str).isin(selected_values)].copy()


def prepare_device_risk_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    prepared_df = dataframe.copy()
    prepared_df["risk_sort"] = prepared_df["risk_level"].map(RISK_ORDER).fillna(len(RISK_ORDER))
    prepared_df = prepared_df.sort_values(
        by=["risk_sort", "attack_rate", "total_events"],
        ascending=[True, False, False],
    )
    return prepared_df.drop(columns=["risk_sort"])


def prepare_attack_summary_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()
    return dataframe.sort_values(by=["total_attack_events", "affected_devices"], ascending=[False, False])


def prepare_protocol_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()
    return dataframe.sort_values(by=["total_events", "attack_events"], ascending=[False, False])


def format_dataframe_for_display(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    display_df = dataframe.copy()
    for column_name in display_df.columns:
        if pd.api.types.is_datetime64_any_dtype(display_df[column_name]):
            display_df[column_name] = display_df[column_name].astype(str)

    return display_df


def render_pipeline_overview(quality_df: pd.DataFrame) -> None:
    st.header("Pipeline Overview")
    render_section_intro(
        "High-level pipeline health metrics from `mart_pipeline_quality_summary`, based on the latest dbt build."
    )

    if quality_df.empty:
        render_empty_state(
            "The pipeline quality mart is empty, so the KPI cards cannot be calculated yet.",
            f"Run the ingestion flow and then `{PREPARE_DATA_COMMAND}` before reopening the dashboard.",
        )
        return

    quality_row = quality_df.iloc[0]
    metric_columns = st.columns(4)
    metric_columns[0].metric("Processed records", int(quality_row["processed_records"]))
    metric_columns[1].metric("Invalid records", int(quality_row["invalid_records"]))
    metric_columns[2].metric("Total records", int(quality_row["total_records"]))
    metric_columns[3].metric("Invalid rate", f"{float(quality_row['invalid_rate']) * 100:.2f}%")

    st.caption(
        "Last processed at: "
        f"{quality_row['last_processed_at']} | Last invalid at: {quality_row['last_invalid_at']}"
    )
    st.caption("This one-row mart is helpful for quickly validating that the local end-to-end pipeline produced expected records.")
    st.dataframe(format_dataframe_for_display(quality_df), width="stretch", hide_index=True)


def render_device_risk_section(device_df: pd.DataFrame, selected_risk_levels: list[str], top_n: int) -> None:
    st.header("Device Risk")
    render_section_intro(
        "Device-level risk view from `mart_device_risk_summary`, combining event volume, attack rate, failure rate, and derived risk bands."
    )

    if device_df.empty:
        render_empty_state(
            "The device risk mart is empty, so there are no device-level insights to display yet.",
            f"Run the pipeline and `{PREPARE_DATA_COMMAND}` to populate `mart_device_risk_summary`.",
        )
        return

    filtered_df = filter_by_values(device_df, "risk_level", selected_risk_levels)
    if filtered_df.empty:
        render_empty_state(
            "No device risk rows match the current filters.",
            "Try selecting more risk levels in the sidebar or rebuild the marts if you expected device data.",
        )
        return

    prepared_df = prepare_device_risk_dataframe(filtered_df)
    chart_columns = st.columns(2)

    risk_counts = (
        filtered_df["risk_level"]
        .value_counts()
        .reindex(RISK_DISPLAY_ORDER, fill_value=0)
        .rename_axis("risk_level")
        .reset_index(name="device_count")
    )
    chart_columns[0].subheader("Device count by risk level")
    chart_columns[0].bar_chart(risk_counts.set_index("risk_level"))
    chart_columns[0].caption("Shows how many devices currently fall into each derived risk band.")

    top_devices = prepared_df.head(top_n).set_index("device_id")[["total_events"]]
    chart_columns[1].subheader(f"Top {min(top_n, len(prepared_df))} devices by total events")
    chart_columns[1].bar_chart(top_devices)
    chart_columns[1].caption("Highlights the busiest devices in the filtered view.")

    st.subheader("Device risk summary table")
    st.caption("Sorted by risk level first, then by attack rate and total event volume.")
    st.dataframe(
        format_dataframe_for_display(prepared_df.head(top_n)),
        width="stretch",
        hide_index=True,
    )


def render_attack_summary_section(
    attack_df: pd.DataFrame, selected_attack_types: list[str], top_n: int
) -> None:
    st.header("Attack Summary")
    render_section_intro(
        "Attack-focused summary from `mart_attack_summary`, useful for understanding the most common attack types and affected devices."
    )

    if attack_df.empty:
        render_empty_state(
            "The attack summary mart is empty, so attack-level trends are not available yet.",
            f"Run the pipeline and `{PREPARE_DATA_COMMAND}` to populate `mart_attack_summary`.",
        )
        return

    filtered_df = filter_by_values(attack_df, "attack_type", selected_attack_types)
    if filtered_df.empty:
        render_empty_state(
            "No attack summary rows match the current filters.",
            "Try selecting more attack types in the sidebar or rebuild the marts if you expected attack data.",
        )
        return

    prepared_df = prepare_attack_summary_dataframe(filtered_df)

    st.subheader("Attack events by attack type")
    st.bar_chart(prepared_df.head(top_n).set_index("attack_type")[["total_attack_events"]])
    st.caption("Ranks attack types by total detected attack events in the current filtered view.")

    st.subheader("Attack summary table")
    st.caption("Sorted by total attack events and then by affected device count.")
    st.dataframe(
        format_dataframe_for_display(prepared_df.head(top_n)),
        width="stretch",
        hide_index=True,
    )


def render_protocol_metrics_section(
    protocol_df: pd.DataFrame, selected_protocols: list[str], top_n: int
) -> None:
    st.header("Protocol Metrics")
    render_section_intro(
        "Protocol-level metrics from `mart_protocol_metrics`, showing how traffic volume and attack activity are distributed across protocols."
    )

    if protocol_df.empty:
        render_empty_state(
            "The protocol metrics mart is empty, so protocol-level analytics are not available yet.",
            f"Run the pipeline and `{PREPARE_DATA_COMMAND}` to populate `mart_protocol_metrics`.",
        )
        return

    filtered_df = filter_by_values(protocol_df, "protocol", selected_protocols)
    if filtered_df.empty:
        render_empty_state(
            "No protocol metric rows match the current filters.",
            "Try selecting more protocols in the sidebar or rebuild the marts if you expected protocol data.",
        )
        return

    prepared_df = prepare_protocol_dataframe(filtered_df)

    st.subheader("Total events by protocol")
    st.bar_chart(prepared_df.head(top_n).set_index("protocol")[["total_events"]])
    st.caption("Shows protocol traffic volume for the current filtered view.")

    st.subheader("Protocol metrics table")
    st.caption("Sorted by total events and then by attack event count.")
    st.dataframe(
        format_dataframe_for_display(prepared_df.head(top_n)),
        width="stretch",
        hide_index=True,
    )


def render_raw_mart_tables(
    quality_df: pd.DataFrame,
    device_df: pd.DataFrame,
    attack_df: pd.DataFrame,
    protocol_df: pd.DataFrame,
    selected_risk_levels: list[str],
    selected_attack_types: list[str],
    selected_protocols: list[str],
    top_n: int,
) -> None:
    st.header("Raw Mart Tables")
    render_section_intro(
        "Direct table previews from the current dbt marts. These are useful for debugging filters, validating transformations, and capturing portfolio screenshots."
    )

    mart_views = [
        (
            "mart_device_risk_summary",
            prepare_device_risk_dataframe(filter_by_values(device_df, "risk_level", selected_risk_levels)).head(top_n),
        ),
        (
            "mart_attack_summary",
            prepare_attack_summary_dataframe(filter_by_values(attack_df, "attack_type", selected_attack_types)).head(top_n),
        ),
        (
            "mart_protocol_metrics",
            prepare_protocol_dataframe(filter_by_values(protocol_df, "protocol", selected_protocols)).head(top_n),
        ),
        ("mart_pipeline_quality_summary", quality_df),
    ]

    for table_name, dataframe in mart_views:
        st.subheader(table_name)
        if dataframe.empty:
            render_empty_state(
                f"`{table_name}` has no rows for the current dashboard state.",
                "This can happen when a mart is empty or when the active sidebar filters remove all rows.",
            )
            continue
        st.caption("Table preview for the active dashboard state.")
        st.dataframe(format_dataframe_for_display(dataframe), width="stretch", hide_index=True)


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    st.write(
        "Stage 6C polished analytics dashboard for the IoT Log Intelligence Pipeline. "
        "It turns the dbt marts in PostgreSQL into a clean, portfolio-ready UI with KPIs, filters, charts, and raw mart previews."
    )
    st.caption(
        "Preparation reminder: run the pipeline, load PostgreSQL, and build the dbt marts before using the dashboard."
    )

    if not render_connection_status():
        st.stop()

    try:
        missing_tables = get_missing_tables(MART_TABLES)
    except psycopg.Error as error:
        st.error("Unable to inspect dbt marts in PostgreSQL.")
        st.caption(f"Metadata error: {error.__class__.__name__}")
        st.stop()
    except Exception as error:
        st.error("The dashboard could not inspect PostgreSQL metadata.")
        st.caption(f"Dashboard error: {error.__class__.__name__}")
        st.stop()

    if missing_tables:
        missing_list = ", ".join(f"`{table_name}`" for table_name in missing_tables)
        st.warning(
            "Some dbt marts are missing, so a few dashboard sections may stay empty until the models are rebuilt."
        )
        st.caption(f"Missing marts: {missing_list}")
        st.caption(f"Recommended fix: run `{PREPARE_DATA_COMMAND}` after loading pipeline data into PostgreSQL.")

    mart_frames, mart_errors = load_mart_frames(missing_tables)
    for table_name, error_name in mart_errors.items():
        render_query_warning(table_name, error_name)

    device_df = mart_frames["mart_device_risk_summary"]
    attack_df = mart_frames["mart_attack_summary"]
    protocol_df = mart_frames["mart_protocol_metrics"]
    quality_df = mart_frames["mart_pipeline_quality_summary"]

    selected_risk_levels, selected_attack_types, selected_protocols, top_n = render_sidebar(
        device_df=device_df,
        attack_df=attack_df,
        protocol_df=protocol_df,
    )

    render_pipeline_overview(quality_df=quality_df)
    render_device_risk_section(
        device_df=device_df,
        selected_risk_levels=selected_risk_levels,
        top_n=top_n,
    )
    render_attack_summary_section(
        attack_df=attack_df,
        selected_attack_types=selected_attack_types,
        top_n=top_n,
    )
    render_protocol_metrics_section(
        protocol_df=protocol_df,
        selected_protocols=selected_protocols,
        top_n=top_n,
    )
    render_raw_mart_tables(
        quality_df=quality_df,
        device_df=device_df,
        attack_df=attack_df,
        protocol_df=protocol_df,
        selected_risk_levels=selected_risk_levels,
        selected_attack_types=selected_attack_types,
        selected_protocols=selected_protocols,
        top_n=top_n,
    )


if __name__ == "__main__":
    main()
