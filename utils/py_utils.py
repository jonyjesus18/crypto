import pandas as pd
from loguru import logger
import plotly.graph_objects as go

"""
Miscenallaneous helper classes
"""


class StrValueEnum:
    def __init__(self, enum_class):
        self._enum_class = enum_class

    def __getattr__(self, name):
        return self._enum_class[name].value


def index_slice(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Filters a multi-index DataFrame based on the provided key-value pairs using pd.xs.

    Parameters:
    - df (pd.DataFrame): Input DataFrame with multi-index columns.
    - **kwargs: Key-value pairs specifying levels and corresponding values to filter.
      Values can be a single value or a list of values.

    Returns:
    - pd.DataFrame: Filtered DataFrame.
    """
    try:
        filter_df = df.copy()

        # Ensure columns are a MultiIndex
        if not isinstance(filter_df.columns, pd.MultiIndex):
            raise ValueError("The DataFrame must have a MultiIndex for its columns.")

        for level, values in kwargs.items():
            if not isinstance(values, (list, tuple)):
                values = [values]

            # Filter columns using pd.xs for each value in the list
            slices = []
            for value in values:
                try:
                    slice_df = filter_df.xs(
                        value, level=level, axis=1, drop_level=False
                    )
                    slices.append(slice_df)
                except KeyError:
                    logger.warning(f"Value '{value}' not found in level '{level}'.")

            if not slices:
                logger.warning(
                    f"No matching values found for level '{level}'. Returning an empty DataFrame."
                )
                return pd.DataFrame()

            # Concatenate all slices
            filter_df = pd.concat(slices, axis=1)

        return filter_df

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An unexpected error occurred during filtering: {e}")
        return pd.DataFrame()


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


import pandas as pd


def keep_levels(df: pd.DataFrame, levels_to_keep) -> pd.DataFrame:
    """
    Retains only the specified levels in the MultiIndex columns of a DataFrame.

    Parameters:
        df (pd.DataFrame): Input DataFrame with MultiIndex columns.
        levels_to_keep (str or list): A level name or list of level names to retain in the MultiIndex.

    Returns:
        pd.DataFrame: A DataFrame with only the specified levels retained in the MultiIndex columns.
    """
    # Ensure the DataFrame has MultiIndex columns
    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError("The DataFrame does not have MultiIndex columns.")

    # Normalize levels_to_keep to a list
    if isinstance(levels_to_keep, str):
        levels_to_keep = [levels_to_keep]

    # Validate levels_to_keep
    column_levels = df.columns.names
    invalid_levels = [level for level in levels_to_keep if level not in column_levels]
    if invalid_levels:
        raise ValueError(
            f"Invalid levels specified: {invalid_levels}. Available levels are: {column_levels}"
        )

    # Retain only the specified levels
    retained_columns = df.columns.droplevel(
        [level for level in column_levels if level not in levels_to_keep]
    )
    return df.copy().set_axis(retained_columns, axis=1)


def plot_timeseries(df: pd.DataFrame, cols_to_plot=None, show_buy=False):
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
        if col not in ["buy"]:
            trace = go.Scatter(
                x=df.index,
                y=df[col],
                mode="lines",
                name=str(col),  # This joins the gaps
            )
            traces.append(trace)

    shapes = []
    if show_buy and "buy" in df.columns:
        buy_indices = df.index[df["buy"] == 1]
        for timestamp in buy_indices:
            shapes.append(
                dict(
                    type="line",
                    xref="x",
                    yref="paper",
                    x0=timestamp,
                    x1=timestamp,
                    y0=0,
                    y1=1,
                    line=dict(color="green", width=2, dash="dash"),
                )
            )
    # Layout for the plot
    layout = go.Layout(
        title="Timeseries Plot",
        xaxis=dict(title="Time"),
        yaxis=dict(title="Value"),
        showlegend=True,
        shapes=shapes if show_buy else None,
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
