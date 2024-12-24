import os
import pandas as pd
from data_hooks.data_hook import Datahook

DEMO_DATA_PATH = "demo_data"


class DemoDatahook(Datahook):
    def get_data(self):
        raw_data = self.get_raw_data()
        pivot_df = self._process_data(raw_data)
        return pivot_df

    def _process_data(self, raw_df: pd.DataFrame):
        pivot_df = pd.pivot(data=raw_df, columns=["series", "coin"], values="price")

        pivot_df.index = pd.to_datetime(pivot_df.index, format="mixed")
        ts_resample = pivot_df.resample("5min").mean().dropna().round(3)
        return ts_resample

    def get_raw_data(self):
        df = pd.DataFrame()
        for file in os.listdir(path=DEMO_DATA_PATH):
            coin_df = pd.read_csv(
                filepath_or_buffer=f"{DEMO_DATA_PATH}/{file}", index_col="datetime"
            )
            coin_df = coin_df[["price", "series"]]
            coin = file.replace(".csv", "")
            coin_df["coin"] = coin
            df = pd.concat([df, coin_df])

        return df
