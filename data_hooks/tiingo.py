import os
import pandas as pd
from data_hooks.data_hook import Datahook
from db.mongodb import MongoDB

DEMO_DATA_PATH = "demo_data"


class Tiingo(Datahook):
    def get_data(self, start_date: str, end_date: str):
        raw_data = self.get_raw_data(start_date, end_date)
        pivot_df = self._process_data(raw_data)
        return pivot_df

    def _process_data(self, raw_df: pd.DataFrame):
        df = raw_df[["datetime", "ticker", "field", "value"]]
        df["datetime"] = pd.to_datetime(df["datetime"])
        df_pivot = pd.pivot_table(
            df, values="value", index="datetime", columns=["ticker", "field"]
        )
        return df_pivot

    def get_raw_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = MongoDB().query_timeseries(
            database="prices",
            collection="coin_timeseries",
            start_time=start_date,
            end_time=end_date,
        )

        return pd.DataFrame(df)
