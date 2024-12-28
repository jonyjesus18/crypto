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


def collapse_multi_index_cols(df: pd.DataFrame, join_str: str = "_") -> pd.DataFrame:
    """
    Collapse the vertical levels of a MultiIndex on the columns by concatenating the column levels.

    Args:
        df (pd.DataFrame): The DataFrame with a MultiIndex on columns.
        join_str (str): The string used to join the column index levels.

    Returns:
        pd.DataFrame: A DataFrame with collapsed MultiIndex columns.
    """
    if isinstance(df.columns, pd.MultiIndex):
        # Collapse the column index levels into a single level by joining with the join_str
        df.columns = [join_str.join(map(str, col)) for col in df.columns]

    return df


def keep_levels(df: pd.DataFrame, levels_to_keep) -> pd.DataFrame:
    """
    Keeps only the specified levels in a MultiIndex column DataFrame.

    Parameters:
        df (pd.DataFrame): The input DataFrame with MultiIndex columns.
        levels_to_keep (str or list): A level name or list of level names to retain in the MultiIndex.

    Returns:
        pd.DataFrame: A DataFrame with only the specified levels retained.
    """
    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError("The DataFrame does not have MultiIndex columns.")

    # Normalize levels_to_keep to a list if it's a string
    if isinstance(levels_to_keep, str):
        levels_to_keep = [levels_to_keep]

    # Validate levels_to_keep
    invalid_levels = [
        level for level in levels_to_keep if level not in df.columns.names
    ]
    if invalid_levels:
        raise ValueError(
            f"Invalid levels specified: {invalid_levels}. Available levels are: {df.columns.names}"
        )

    # Get index of levels to keep
    levels_to_keep_idx = [df.columns.names.index(level) for level in levels_to_keep]

    # Reindex the MultiIndex columns
    new_columns = df.columns.droplevel(
        [i for i in range(len(df.columns.names)) if i not in levels_to_keep_idx]
    )
    df = df.copy()
    df.columns = new_columns

    return df


import plotly.graph_objects as go


def plot_timeseries(df, cols_to_plot=None):
    """
    Plots the timeseries data from a DataFrame using Plotly.

    Parameters:
    - df: pandas DataFrame containing the timeseries data.
    - cols_to_plot: list of column names to plot, if None, all columns will be plotted.
    """
    # If no columns are specified, plot all columns
    if cols_to_plot is None:
        cols_to_plot = df.columns.tolist()

    # Create a list of traces for the plot
    traces = []

    for col in cols_to_plot:
        # Create the trace for each column

        trace = go.Scatter(
            x=df.index, y=df[col], mode="lines", name=str(col)  # This joins the gaps
        )
        traces.append(trace)

    # Layout for the plot
    layout = go.Layout(
        title="Timeseries Plot",
        xaxis=dict(title="Time"),
        yaxis=dict(title="Value"),
        showlegend=True,
    )

    # Create the figure and plot it
    fig = go.Figure(data=traces, layout=layout)
    fig.show()


def plot_candlestick(df):
    slice_df = keep_levels(df, levels_to_keep=["field"])
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=slice_df.index,
                open=slice_df["open"],
                high=slice_df["high"],
                low=slice_df["low"],
                close=slice_df["close"],
            )
        ]
    )

    return fig
