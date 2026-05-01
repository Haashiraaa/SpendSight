

# src/core/analysis.py

import logging
from typing import cast, Optional, List, Tuple
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
        self.credit_df = credit_df.assign(
            trans_month=credit_df["trans_month"].astype(str)
        )
        self.debit_df = debit_df.assign(
            trans_month=debit_df["trans_month"].astype(str)
        )

    def get_all_months(self) -> List[str]:
        credit_months = set(self.credit_df["trans_month"].unique())
        debit_months = set(self.debit_df["trans_month"].unique())
        all_months = sorted(credit_months | debit_months)  # union
        return all_months

    def get_income_series(self) -> List[float]:
        months = self.get_all_months()
        return (
            self.credit_df[self.credit_df["description"] != "Savings"]
            .groupby("trans_month")["credit(₦)"]
            .sum()
            .reindex(months, fill_value=0)
            .tolist()
        )

    def get_expenses_series(self) -> List[float]:
        months = self.get_all_months()
        return (
            self.debit_df
            .groupby("trans_month")["debit(₦)"]
            .sum()
            .reindex(months, fill_value=0)
            .tolist()
        )

    def get_net_savings(self) -> List[float]:

        income = self.get_income_series()
        expenses = self.get_expenses_series()

        return [i - e for i, e in zip(income, expenses)]

    def get_category_spendings(self) -> Tuple[List[str], List[float]]:
        grouped = (
            self.debit_df
            .groupby("description")["debit(₦)"]
            .sum()
            .sort_values(ascending=False)
        )
        total = grouped.sum()
        # Collapse anything under 2% into "Other"
        mask = grouped / total < 0.02
        # Add tiny ones to Shopping & Bills
        grouped["Shopping & Bills"] += grouped[mask].sum()
        result = grouped[~mask]  # drop the tiny ones

        return result.index.tolist(), result.values.tolist()

    def get_top_spend_category(self) -> Tuple[str, float]:

        top_spend = (
            self.debit_df
            .groupby("description")["debit(₦)"]
            .sum()
            .reset_index()
            .sort_values(by="debit(₦)", ascending=False)
        ).iloc[0]
        return str(top_spend["description"]), float(top_spend["debit(₦)"])
