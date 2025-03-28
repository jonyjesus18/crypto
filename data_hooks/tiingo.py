from functools import lru_cache
import pandas as pd
from typing import Optional
from data_hooks.data_hook import Datahook
from db.timescaledb import TimescaleDB
from loguru import logger


class Tiingo(Datahook):
    def __init__(self):
        self.db = TimescaleDB()

    @lru_cache
    def get_data(
        self, start_date: str = "2024-06-01", end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Get processed data from Tiingo."""
        raw_data = self.get_raw_data(start_date, end_date)
        return self._process_data(raw_data)

    @lru_cache
    def get_raw_data(
        self, start_date: str, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Get raw data from database."""
        query = self._build_query(start_date, end_date)
        try:
            return self.db.query_db(query)  # Using sync method instead of async
        except Exception as e:
            logger.error(f"Failed to get data: {e}")
            raise

    def _process_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into pivot table format."""
        df = raw_df[["datetime", "ticker", "field", "value"]].copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df_deduped = df.drop_duplicates(subset=["datetime", "ticker", "field", "value"])
        return pd.pivot_table(
            df_deduped, values="value", index="datetime", columns=["ticker", "field"]
        )

    def _build_query(self, start_date: str, end_date: Optional[str] = None) -> str:
        """Build SQL query string."""
        query = f"""
            SELECT *
            FROM bars
            WHERE source = 'tiingo'
            AND datetime >= '{start_date}'
        """
        if end_date:
            query += f" AND datetime <= '{end_date}'"
        return query
