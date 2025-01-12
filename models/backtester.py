from dataclasses import dataclass
import pandas as pd
import plotly.graph_objects as go
from typing import List


# 1. DataHandler class for managing data
class DataHandler:
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def get_data(self) -> pd.DataFrame:
        return self.data

    def get_price(self, index: int) -> pd.Series:
        return self.data.iloc[index]

    def get_high(self, index: int) -> float:
        return self.data["high"].iloc[index]

    def get_low(self, index: int) -> float:
        return self.data["low"].iloc[index]

    def get_open(self, index: int) -> float:
        return self.data["open"].iloc[index]

    def get_close(self, index: int) -> float:
        return self.data["close"].iloc[index]


# 2. SignalGenerator class for managing signals
class SignalGenerator:
    def __init__(self, signal: pd.Series):
        self.signal = signal

    def get_signal(self, index: int) -> int:
        return self.signal.iloc[index]


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

    def update_cash(self, amount: float):
        self.cash += amount

    def update_value(self, position_value: float):
        self.total_value += position_value

    def get_cash(self) -> float:
        return self.cash

    def get_total_value(self) -> float:
        return self.total_value


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
        self.data_handler = DataHandler(data)
        self.signal_generator = SignalGenerator(signal)
        self.position_manager = PositionManager(max_positions, fee)
        self.portfolio = Portfolio(initial_capital)
        self.profit_target = profit_target
        self.stop_loss = stop_loss
        self.pnl_evolution = pd.DataFrame(columns=["pnl", "date"]).set_index("date")

    def backtest(self) -> dict:
        for i in range(len(self.data_handler.get_data())):
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
        self.portfolio.update_cash(profit)
        print(
            f"Position closed at index {exit_index} with price {exit_price}, Profit: {profit}"
        )

    def _track_pnl(self, index: int):
        total_value = self.portfolio.get_total_value()
        for position in self.position_manager.get_open_positions():
            total_value += (
                position.entry_price * position.size
            )  # Add open position value to total value
        self.pnl_evolution.loc[index] = total_value

    def _results(self) -> dict:
        return {
            "final_cash": self.portfolio.get_cash(),
            "total_value": self.portfolio.get_total_value(),
            "total_trades": len(self.position_manager.get_open_positions()),
        }

    def plot_pnl_evolution(self):
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=self.pnl_evolution.index,
                y=self.pnl_evolution["pnl"],
                mode="lines",
                name="PnL Evolution",
            )
        )
        fig.update_layout(
            title="PnL Evolution",
            xaxis_title="Time (Index)",
            yaxis_title="Portfolio Value",
            template="plotly_dark",
        )
        fig.show()
