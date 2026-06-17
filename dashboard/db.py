from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd
import psycopg
import streamlit as st
from psycopg import sql


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def load_database_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "iot_logs"),
        user=os.getenv("POSTGRES_USER", "iot_user"),
        password=os.getenv("POSTGRES_PASSWORD", "iot_password"),
    )


@st.cache_resource(show_spinner=False)
def get_connection() -> psycopg.Connection:
    config = load_database_config()
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=5,
        autocommit=True,
    )


def ping_database() -> None:
    with get_connection().cursor() as cursor:
        cursor.execute("SELECT 1")


def get_missing_tables(table_names: list[str]) -> list[str]:
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = ANY(%s)
    """
    with get_connection().cursor() as cursor:
        cursor.execute(query, (table_names,))
        existing_tables = {row[0] for row in cursor.fetchall()}

    return [table_name for table_name in table_names if table_name not in existing_tables]


def fetch_table_preview(table_name: str, limit: int = 20) -> pd.DataFrame:
    query = sql.SQL("SELECT * FROM {table_name} LIMIT %s").format(
        table_name=sql.Identifier(table_name)
    )
    with get_connection().cursor() as cursor:
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        columns = [column.name for column in cursor.description or []]

    return pd.DataFrame(rows, columns=columns)
