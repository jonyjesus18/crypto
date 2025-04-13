from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from pandas import Series, DataFrame, DatetimeIndex
import plotly.graph_objects as go
from typing import List, Union, cast


# 1. DataHandler class for managing data
class DataHandler:
    def __init__(self, data: DataFrame):
        self.data = data
        self.index = cast(DatetimeIndex, data.index)

    def get_data(self) -> DataFrame:
        return self.data

    def get_price(self, index: datetime) -> Series:
        return cast(Series, self.data.loc[index])

    def get_high(self, index: Union[int, datetime]) -> float:
        return float(self.data["high"].loc[index])

    def get_low(self, index: Union[int, datetime]) -> float:
        return float(self.data["low"].loc[index])

    def get_open(self, index: Union[int, datetime]) -> float:
        return float(self.data["open"].loc[index])

    def get_close(self, index: Union[int, datetime]) -> float:
        return float(self.data["close"].loc[index])


# 2. SignalGenerator class for managing signals
class SignalGenerator:
    def __init__(self, signal: pd.Series):
        self.signal = signal

    def get_signal(self, index: int) -> int:
        return self.signal.loc[index]


# 3. Position class to represent each open position
@dataclass
class Position:
    entry_index: int
    entry_price: float
    size: int
    fee: float


# 4. PositionManager class for managing open positions
class PositionManager:
    def __init__(self, max_positions: int, fee: float):
        self.max_positions = max_positions
        self.fee = fee
        self.positions: List[Position] = []

    def open_position(self, index: int, entry_price: float, size: int) -> Position:
        fee_amount = entry_price * self.fee
        position = Position(
            entry_index=index, entry_price=entry_price, size=size, fee=fee_amount
        )
        self.positions.append(position)
        return position

    def close_position(self, position: Position, exit_price: float) -> float:
        fee_amount = exit_price * self.fee
        total_exit_value = exit_price - fee_amount
        profit = (total_exit_value - position.entry_price) * position.size
        self.positions.remove(position)
        return profit

    def get_open_positions(self) -> List[Position]:
        return self.positions


# 5. Portfolio class for managing portfolio value and cash
class Portfolio:
    def __init__(self, initial_capital: float):
        self.cash = initial_capital
        self.total_value = initial_capital
        self.positions: List[Position] = []
        self.realized_pnls = 0.0

    def update_cash(self, amount: float):
        self.cash += amount

    def update_value(self, position_value: float):
        self.total_value += position_value

    def get_cash(self) -> float:
        return self.cash

    def get_total_value(self) -> float:
        return self.total_value

    def get_realized_pnls(self) -> float:
        return self.realized_pnls

    def add_realized_pnl(self, pnl: float):
        self.realized_pnls += pnl


# 6. Backtester class for running the backtest
class Backtester:
    def __init__(
        self,
        data: pd.DataFrame,
        signal: pd.Series,
        initial_capital=100,
        max_positions=5,
        profit_target=0.05,
        stop_loss=0.02,
        fee=0.0015,
    ):
        aligned_data, aligned_signal = data.align(signal, join="inner", axis=0)
        self.data_handler = DataHandler(aligned_data)
        self.signal_generator = SignalGenerator(aligned_signal)
        self.position_manager = PositionManager(max_positions, fee)
        self.portfolio = Portfolio(initial_capital)
        self.profit_target = profit_target
        self.stop_loss = stop_loss
        self.pnl_evolution = pd.DataFrame(
            columns=["realized_pnl", "cumulative_pnl", "date"]
        ).set_index("date")

    def backtest(self) -> dict:
        bt_data = self.data_handler.get_data()
        for i in bt_data.index:
            print(
                f"Index {i}: Signal = {self.signal_generator.get_signal(i)}, Cash = {self.portfolio.get_cash()}"
            )

            if (
                self.signal_generator.get_signal(i) == 1
                and len(self.position_manager.get_open_positions())
                < self.position_manager.max_positions
            ):
                self._open_position(i)

            self._check_positions(i)
            self._track_pnl(i)

        return self._results()

    def _open_position(self, index: int):
        open_price = self.data_handler.get_open(index)
        if self.portfolio.get_cash() >= open_price:
            position = self.position_manager.open_position(index, open_price, 1)
            self.portfolio.update_cash(-open_price)
            print(f"Position opened at index {index} with price {open_price}")

    def _check_positions(self, index: int):
        current_high = self.data_handler.get_high(index)
        current_low = self.data_handler.get_low(index)
        close_price = self.data_handler.get_close(index)

        for position in self.position_manager.get_open_positions():
            target_price = position.entry_price * (1 + self.profit_target)
            stop_loss_price = position.entry_price * (1 - self.stop_loss)

            if current_high >= target_price:
                self._close_position(position, index, target_price)
            elif current_low <= stop_loss_price:
                self._close_position(position, index, stop_loss_price)
            elif index == len(self.data_handler.get_data()) - 1:
                self._close_position(position, index, close_price)

    def _close_position(self, position: Position, exit_index: int, exit_price: float):
        profit = self.position_manager.close_position(position, exit_price)
        self.portfolio.add_realized_pnl(profit)
        self.portfolio.update_cash(profit)
        print(
            f"Position closed at index {exit_index} with price {exit_price}, Profit: {profit}"
        )

    def _track_pnl(self, index: int):
        # Track Realized PnL
        realized_pnl = self.portfolio.get_realized_pnls()

        # Track Cumulative PnL (Realized + Unrealized)
        total_value = self.portfolio.get_total_value()
        for position in self.position_manager.get_open_positions():
            total_value += (
                position.entry_price * position.size
            )  # Add open position value to total value
        cumulative_pnl = total_value - self.portfolio.get_cash() - realized_pnl

        # Store values
        self.pnl_evolution.loc[index] = {  # type: ignore
            "realized_pnl": realized_pnl,
            "cumulative_pnl": cumulative_pnl,
        }

    def _results(self) -> dict:
        return {
            "final_cash": self.portfolio.get_cash(),
            "total_value": self.portfolio.get_total_value(),
            "total_trades": len(self.position_manager.get_open_positions()),
        }

    def plot_pnl_evolution(self):
        fig = go.Figure()

        # Plot Realized PnL
        fig.add_trace(
            go.Scatter(
                x=self.pnl_evolution.index,
                y=self.pnl_evolution["realized_pnl"],
                mode="lines",
                name="Realized PnL",
            )
        )

        # Plot Cumulative PnL
        fig.add_trace(
            go.Scatter(
                x=self.pnl_evolution.index,
                y=self.pnl_evolution["cumulative_pnl"],
                mode="lines",
                name="Cumulative PnL",
            )
        )

        # Add Buy markers (where positions are opened)
        buy_indices = [
            position.entry_index
            for position in self.position_manager.get_open_positions()
        ]
        buy_prices = [self.data_handler.get_open(index) for index in buy_indices]
        fig.add_trace(
            go.Scatter(
                x=self.data_handler.get_data().iloc[buy_indices].index,
                y=buy_prices,
                mode="markers",
                marker=dict(symbol="triangle-up", color="green", size=10),
                name="Buy (Open)",
            )
        )

        # Add Sell markers (where positions are closed)
        sell_indices = [
            position.entry_index
            for position in self.position_manager.get_open_positions()
        ]
        sell_prices = [self.data_handler.get_open(index) for index in sell_indices]
        fig.add_trace(
            go.Scatter(
                x=self.data_handler.get_data().iloc[sell_indices].index,
                y=sell_prices,
                mode="markers",
                marker=dict(symbol="triangle-down", color="red", size=10),
                name="Sell (Close)",
            )
        )

        fig.update_layout(
            title="PnL Evolution",
            xaxis_title="Time (Index)",
            yaxis_title="PnL",
            template="plotly_dark",
        )
        fig.show()
