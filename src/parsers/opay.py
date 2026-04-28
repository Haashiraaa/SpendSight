

# src/parsers/opay.py


import logging
from typing import Optional, cast
from src.parsers.base import BaseParser
from haashi_pkg.utility import Logger, FileHandler
from haashi_pkg.data_engine import (
    DataLoader, DataAnalyzer, DataFrame, Series
)


class OpayParser(BaseParser):

    def __init__(
        self,
        logger: Optional[Logger] = None,
        handler: Optional[FileHandler] = None
    ) -> None:

        self.logger = logger or Logger(level=logging.INFO)
        self.handler = handler or FileHandler(logger=self.logger)

        # Initialize analyzer
        self.analyzer = DataAnalyzer(logger=self.logger)

        # Configuration: Patterns to mask with generic descriptions
        self.MASKING_MAP = {
            "Transfer": "Transfers",
            "Mobile|Airtime|SMS|USSD": "Phone & Data",
            "Electricity": "Electricity bill",
            "Card|Merchant": "Purchases",
        }

    def _get_debit_transactions(
        self,
        df: DataFrame,
        save_path: str = "data/cleaned_data/debit_transactions/parquet"
    ) -> None:
        # Filter to debit transactions only
        self.logger.debug("Filtering to debit transactions only")
        initial_count = len(df)

        debit_df = cast(
            DataFrame,
            df[df["credit(₦)"] == "--"]
        )
        self.logger.debug(
            f"Kept {len(debit_df)}/{initial_count} debit transactions")

        # Drop unnecessary columns
        self.logger.debug("Removing unnecessary columns")
        columns_to_drop = [
            "value_date",
            "channel",
            "credit(₦)",
            "balance_after(₦)",
            "transaction_reference",
        ]
        debit_df = debit_df.drop(columns=columns_to_drop)

        # Rename trans._date column
        debit_df = debit_df.rename(columns={"trans._date": "trans_date"})

        # Drop specific transaction types
        self.logger.debug("Removing savings and investment transactions")

        # Patterns for transactions to exclude
        # phone_numbers = "8051021438|9058929223|8111016740|9037527321"
        keywords = "Save|OWealth|Fixed"

        before_drop = len(debit_df)
        # debit_df = drop_description(debit_df, "description", phone_numbers)
        debit_df = self._drop_description(
            debit_df, "description", keywords)
        dropped = before_drop - len(debit_df)

        self.logger.debug(f"Removed {dropped} excluded transactions")

        # Mask sensitive/specific descriptions with generic labels
        self.logger.debug("Masking transaction descriptions")
        for pattern, generic_desc in self.MASKING_MAP.items():
            self._mask_description(
                debit_df, "description", pattern, generic_desc)

        # Convert data types
        self.logger.debug("Converting data types")
        debit_df["trans_date"] = self.analyzer.convert_datetime(
            cast(Series, debit_df["trans_date"]))
        debit_df["description"] = debit_df["description"].astype(
            "category")
        debit_df["debit(₦)"] = self.analyzer.convert_numeric(
            cast(Series, debit_df["debit(₦)"]))

        # Add derived column: transaction month
        self.logger.debug("Adding transaction month column")
        debit_df["trans_month"] = debit_df["trans_date"].dt.to_period("M")

        # Final validation
        self.logger.debug("Validating cleaned data")
        self.analyzer.validate_columns_exist(
            debit_df,
            ["trans_date", "description", "debit(₦)", "trans_month"]
        )

        self.logger.info(
            f"Cleaning completed: {len(debit_df)} transactions retained")
        self.logger.info(
            f"Date range: {debit_df['trans_date'].min()} to {debit_df['trans_date'].max()}")
        self.logger.info(f"Categories: {debit_df['description'].nunique()}")

        self._save_data(debit_df, save_path, self.logger, self.handler)

        self.analyzer.inspect_dataframe(debit_df)

    def parse_data(
        self,
        file_path: str,
        rows_to_skip: int = 6
    ) -> None:

        self.logger.info(f"Loading data from {file_path}")
        self.logger.debug(f"Skipping {rows_to_skip} header rows")

        # Load data
        loader = DataLoader(file_path, logger=self.logger)
        bank_st_df = loader.load_excel_single(skip_rows=rows_to_skip)

        self.logger.info(f"Loaded {len(bank_st_df)} transactions")
        self.logger.debug("Starting data cleaning...")

        # Normalize column names
        self.logger.debug("Normalizing column names")
        bank_st_df = self.analyzer.normalize_column_names(bank_st_df)
        bank_st_df.columns = bank_st_df.columns.str.replace(" ", "_")

        self._get_debit_transactions(bank_st_df)
