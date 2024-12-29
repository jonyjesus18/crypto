from functools import lru_cache
import pandas as pd
from data_hooks.data_hook import Datahook
from db.mongodb import MongoDB


class Tiingo(Datahook):
    @lru_cache
    def get_data(self, start_date: str, end_date: str | None = None) -> pd.DataFrame:
        raw_data = self.get_raw_data(start_date, end_date)
        pivot_df = self._process_data(raw_data)
        return pivot_df

    def _process_data(self, raw_df: pd.DataFrame):
        df = raw_df[["datetime", "ticker", "field", "value"]].copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df_deduped = df.drop_duplicates(subset=["datetime", "ticker", "field", "value"])
        df_pivot = pd.pivot_table(
            df_deduped, values="value", index="datetime", columns=["ticker", "field"]
        )
        return df_pivot

    @lru_cache
    def get_raw_data(self, start_date: str, end_date: str | None) -> pd.DataFrame:
        df = MongoDB().query_timeseries(
            database="prices",
            collection="coin_timeseries",
            start_time=start_date,
            end_time=end_date,
        )

        return pd.DataFrame(df)
