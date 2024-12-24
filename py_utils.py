import pandas as pd
from loguru import logger

"""
Miscenallaneous helper classes
"""


class StrValueEnum:
    def __init__(self, enum_class):
        self._enum_class = enum_class

    def __getattr__(self, name):
        return self._enum_class[name].value


def index_slice(df, **kwargs) -> pd.DataFrame:
    filter_df = df.copy()
    for key, arg in kwargs.items():
        if isinstance(arg, list):
            slices_df = pd.DataFrame()
            for sub_arg in arg:
                try:
                    slice_df: pd.DataFrame = filter_df.xs(sub_arg, level=key, axis=1, drop_level=False)  # type: ignore
                    slices_df = pd.concat([slices_df, slice_df], axis=1)

                except:
                    logger.warning(f"No {sub_arg} in {key} level")

            filter_df = slices_df
        else:
            try:
                filter_df: pd.DataFrame = filter_df.xs(arg, level=key, axis=1, drop_level=False)  # type: ignore
            except:
                logger.warning(f"No {arg} in {key} level")

    return filter_df
