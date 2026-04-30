

# src/parsers/opay.py


import logging
from typing import Optional, Tuple, cast
from src.parsers.base import BaseParser
from haashi_pkg.utility import Logger, FileHandler, ScreenUtil as su
from haashi_pkg.data_engine import (
    DataLoader, DataAnalyzer, DataFrame, Series
)

MASKING_MAP = {
    # Income
    "Transfer from":  "Income",
    "Bank Deposit":   "Income",
    "Cash Deposit":   "Income",
    "Add Money":      "Income",
    "Reversal":       "Income",

    # Transfers Out
    "Transfer to":    "Transfers",

    # Savings & Investments
    "SafeBox":        "Savings",
    "OWealth":        "Savings",
    "Targets":        "Savings",
    "Spend & Save":   "Savings",
    "Fixed":          "Savings",

    # Phone & Data
    "Airtime":        "Phone & Data",
    "Mobile Data":    "Phone & Data",
    "USSD Charge":    "Phone & Data",
    "SMS Subscription": "Phone & Data",

    # Utilities
    "Electricity":    "Utilities",
    "TV":             "Utilities",

    # Shopping & Bills
    "Online Payment":     "Shopping & Bills",
    "OPay Card Payment":  "Shopping & Bills",
    "Gift Card":          "Shopping & Bills",

    # Cash
    "Cash Withdraw":  "Cash Withdrawal",

    # Bank Charges
    "Stamp Duty":     "Bank Charges",

    # Betting
    "Betting":        "Betting",
}


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
        self.MASKING_MAP = MASKING_MAP

    def _get_debit_transactions(
        self,
        df: DataFrame,
        save_path: str = "data/cleaned_data/debit_transactions.parquet"
    ) -> DataFrame:
        # Filter to debit transactions only
        su.space()
        self.logger.info("Filtering to debit transactions only")
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

        # self._save_data(debit_df, save_path, self.logger, self.handler)

        # self.analyzer.inspect_dataframe(debit_df)
        # print(debit_df["description"].unique())

        return debit_df

    def _get_credit_transactions(
        self,
        df: DataFrame,
        save_path: str = "data/cleaned_data/credit_transactions.parquet"
    ) -> DataFrame:
        # Filter to debit transactions only
        su.space()
        self.logger.info("Filtering to credit transactions only")
        initial_count = len(df)

        credit_df = cast(
            DataFrame,
            df[df["debit(₦)"] == "--"]
        )
        self.logger.debug(
            f"Kept {len(credit_df)}/{initial_count} credit transactions")

        # Drop unnecessary columns
        self.logger.debug("Removing unnecessary columns")
        columns_to_drop = [
            "value_date",
            "channel",
            "debit(₦)",
            "balance_after(₦)",
            "transaction_reference",
        ]
        credit_df = credit_df.drop(columns=columns_to_drop)

        # Rename trans._date column
        credit_df = credit_df.rename(columns={"trans._date": "trans_date"})

        # Mask sensitive/specific descriptions with generic labels
        self.logger.debug("Masking transaction descriptions")
        for pattern, generic_desc in self.MASKING_MAP.items():
            self._mask_description(
                credit_df, "description", pattern, generic_desc)

        # Convert data types
        self.logger.debug("Converting data types")
        credit_df["trans_date"] = self.analyzer.convert_datetime(
            cast(Series, credit_df["trans_date"]))
        credit_df["description"] = credit_df["description"].astype(
            "category")
        credit_df["credit(₦)"] = self.analyzer.convert_numeric(
            cast(Series, credit_df["credit(₦)"]))

        # Add derived column: transaction month
        self.logger.debug("Adding transaction month column")
        credit_df["trans_month"] = credit_df["trans_date"].dt.to_period("M")

        # Final validation
        self.logger.debug("Validating cleaned data")
        self.analyzer.validate_columns_exist(
            credit_df,
            ["trans_date", "description", "credit(₦)", "trans_month"]
        )

        self.logger.info(
            f"Cleaning completed: {len(credit_df)} transactions retained")
        self.logger.info(
            f"Date range: {credit_df['trans_date'].min()} to {credit_df['trans_date'].max()}")
        self.logger.info(f"Categories: {credit_df['description'].nunique()}")

        # self._save_data(credit_df, save_path, self.logger, self.handler)

        # self.analyzer.inspect_dataframe(credit_df)
        # print(credit_df["description"].unique())

        return credit_df

    def parse_data(
        self,
        file_path: str,
        rows_to_skip: int = 6
    ) -> Tuple[DataFrame, ...]:

        self.logger.info(f"Loading data from {file_path}")
        self.logger.debug(f"Skipping {rows_to_skip} header rows")

        file_path = str(self.handler.get_script_dir() / file_path)

        # Load data
        loader = DataLoader(file_path, logger=self.logger)
        bank_st_df = loader.load_excel_single(skip_rows=rows_to_skip)

        self.logger.info(f"Loaded {len(bank_st_df)} transactions")
        self.logger.debug("Starting data cleaning...")

        # Normalize column names
        self.logger.debug("Normalizing column names")
        bank_st_df = self.analyzer.normalize_column_names(bank_st_df)
        bank_st_df.columns = bank_st_df.columns.str.replace(" ", "_")

        debit_df = self._get_debit_transactions(bank_st_df)

        credit_df = self._get_credit_transactions(bank_st_df)

        return debit_df, credit_df
