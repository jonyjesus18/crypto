import asyncpg
import pandas as pd
from typing import List
from loguru import logger
import os
import psycopg2
from sqlalchemy import create_engine, text


import os
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger


class TimescaleDB:
    def __init__(self):
        # Get connection parameters from env
        self.user = os.getenv("TIMESCALE_USER")
        self.password = os.getenv("TIMESCALE_PASSWORD")
        self.host = os.getenv("TIMESCALE_HOST")
        self.port = os.getenv("TIMESCALE_PORT")
        self.database = os.getenv("TIMESCALE_DB")

        if not all([self.user, self.password, self.host, self.port, self.database]):
            raise ValueError(
                "Missing required environment variables for TimescaleDB connection"
            )

        # Build connection string
        conn_str = f"postgresql://{self.user}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"  # type: ignore
        self.engine = create_engine(conn_str, connect_args={"sslmode": "require"})

        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {str(e)}")
            raise

    def query_db(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not query:
            raise ValueError("Query string cannot be empty")

        try:
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise

    async def copy_dataframe_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: List[str],
        schema: str = "public",
    ) -> int:
        CONNECTION = os.getenv("TIMESCALE_DB_CONN")

        """Upload DataFrame using native PostgreSQL COPY."""
        if df.empty:
            raise ValueError("DataFrame is empty")
        if len(columns) != len(df.columns):
            raise ValueError("Column count mismatch")

        # Convert datetime64 columns to Python datetime
        df = df.copy()
        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = df[col].map(lambda x: x.to_pydatetime())

        # Convert to records
        records = [tuple(x) for x in df.to_numpy()]

        conn = await asyncpg.connect(CONNECTION)
        try:
            total_rows = 0
            async with conn.transaction():
                total_rows = await conn.copy_records_to_table(
                    table_name, records=records, schema_name=schema, columns=columns
                )
            print(f"Copied {total_rows} rows to {schema}.{table_name}")
            return total_rows
        finally:
            await conn.close()
