

# src/core/analysis.py

import logging
from typing import cast, Optional
from src.models.aliases import AnalysisLike
from haashi_pkg.utility import Logger, FileHandler
from haashi_pkg.data_engine import DataLoader, DataAnalyzer, Series


class Analysis:

    def __init__(
        self,
        logger: Optional[Logger] = None,
        handler: Optional[FileHandler] = None
    ) -> None:
        self.logger = logger or Logger(logging.INFO)
        self.handler = handler or FileHandler(logger=self.logger)
        self.script_dir = self.handler.get_script_dir()
        self.file_path = str(
            self.script_dir / "data/cleaned_bank_statement.parquet")

    def analyze_data(self) -> AnalysisLike:

        # Initialize analyzer and load data
        analyzer = DataAnalyzer(logger=self.logger)
        loader = DataLoader(self.file_path, logger=self.logger)
        bank_st_df = loader.load_parquet_single()

        self.logger.debug(f"Loaded {len(bank_st_df)} transactions")
        self.logger.debug("Performing aggregations...")

        # Aggregate by month
        monthly_spending = (
            analyzer.aggregate(bank_st_df, "debit(₦)", "trans_month", "sum")
            .reset_index()
            .rename(columns={
                "trans_month": "months",
                "debit(₦)": "total_spending",
            })
            .sort_values("months")
        )

        self.logger.debug(
            f"Calculated spending across {len(monthly_spending)} months")

        # Aggregate by category
        spend_by_category = (
            analyzer.aggregate(bank_st_df, "debit(₦)", "description", "sum")
            .reset_index()
            .rename(columns={
                "description": "category",
                "debit(₦)": "total_spending",
            })
            .sort_values("total_spending", ascending=False)
        )

        self.logger.debug(
            f"Calculated spending across {len(spend_by_category)} categories")

        # Calculate summary statistics
        monthly_avg = float(
            cast(Series, monthly_spending["total_spending"]).median())
        max_expense = float(
            cast(Series, bank_st_df["debit(₦)"]).max())  # type: ignore
        transaction_count = len(bank_st_df)

        self.logger.info("Aggregations completed successfully")
        self.logger.info(f"  Median monthly spending: ₦{monthly_avg:,.2f}")
        self.logger.info(f"  Total transactions: {transaction_count}")
        self.logger.info(f"  Largest expense: ₦{max_expense:,.2f}")

        return (
            monthly_spending,
            spend_by_category,
            monthly_avg,
            transaction_count,
            max_expense
        )
