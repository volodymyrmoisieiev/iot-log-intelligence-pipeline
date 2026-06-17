from __future__ import annotations

import psycopg
import streamlit as st

from db import fetch_table_preview, get_missing_tables, load_database_config, ping_database

PAGE_TITLE = "IoT Log Intelligence Dashboard"
MART_TABLES = [
    "mart_device_risk_summary",
    "mart_attack_summary",
    "mart_protocol_metrics",
    "mart_pipeline_quality_summary",
]


@st.cache_data(ttl=30, show_spinner=False)
def load_preview(table_name: str, limit: int = 20):
    return fetch_table_preview(table_name=table_name, limit=limit)


def render_connection_status() -> bool:
    config = load_database_config()
    st.subheader("Connection status")

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


def render_kpis() -> None:
    st.subheader("Pipeline quality KPIs")

    try:
        quality_df = load_preview("mart_pipeline_quality_summary", limit=1)
    except psycopg.Error as error:
        st.warning(
            "The pipeline quality mart is not ready yet. Run `docker compose run --build --rm dbt dbt run` "
            "and try again."
        )
        st.caption(f"Query error: {error.__class__.__name__}")
        return
    except Exception as error:
        st.error("Unable to render pipeline quality KPIs right now.")
        st.caption(f"Dashboard error: {error.__class__.__name__}")
        return

    if quality_df.empty:
        st.warning("`mart_pipeline_quality_summary` exists, but it does not contain any rows yet.")
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


def render_preview(table_name: str) -> None:
    st.subheader(table_name)

    try:
        preview_df = load_preview(table_name)
    except psycopg.Error as error:
        st.warning(
            f"Unable to read `{table_name}` right now. Confirm PostgreSQL is up and run `dbt run` first."
        )
        st.caption(f"Query error: {error.__class__.__name__}")
        return
    except Exception as error:
        st.error(f"Unable to render `{table_name}` right now.")
        st.caption(f"Dashboard error: {error.__class__.__name__}")
        return

    if preview_df.empty:
        st.info(f"`{table_name}` is available, but no rows were returned yet.")
        return

    st.dataframe(preview_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    st.write(
        "Foundational Streamlit dashboard for the IoT Log Intelligence Pipeline. "
        "It reads dbt marts from PostgreSQL and exposes a simple operational overview."
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
            "Some dbt marts are not available yet. Run `docker compose run --build --rm dbt dbt run` "
            f"before using the dashboard. Missing tables: {missing_list}."
        )

    render_kpis()

    st.subheader("Mart previews")
    st.write("Simple table previews for the current analytics marts.")
    for table_name in MART_TABLES:
        render_preview(table_name)


if __name__ == "__main__":
    main()
