

# src/core/analysis.py

import logging
from typing import cast, Optional, List

from haashi_pkg.utility import Logger
from haashi_pkg.data_engine import Series, DataFrame


class FinanceAnalysis:

    def __init__(
        self,
        debit_df: DataFrame,
        credit_df: DataFrame,
        logger: Optional[Logger] = None,
    ) -> None:

        self.logger = logger or Logger(logging.INFO)
        self.debit_df = debit_df
        self.credit_df = credit_df

    def get_total_income(self) -> float:

        income = cast(
            DataFrame,
            self.credit_df[self.credit_df["description"] != "Savings"]
        )
        return float(income["credit(₦)"].sum())

    def get_income_series(self) -> List[float]:
        return (
            self.credit_df[self.credit_df["description"] != "Savings"]
            .groupby("trans_month")["credit(₦)"]
            .sum()
            .tolist()
        )

    def get_total_expenses(self) -> float:
        return float(self.debit_df["debit(₦)"].sum())

    def get_expenses_series(self) -> List[float]:
        return (
            self.debit_df
            .groupby("trans_month")["debit(₦)"]
            .sum()
            .tolist()
        )
