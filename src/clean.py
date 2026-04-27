

# src/clean.py


import logging
from typing import Optional, cast
from haashi_pkg.utility import Logger, FileHandler
from haashi_pkg.data_engine import (
    DataLoader, DataAnalyzer, DataSaver, DataFrame, Series
)


class CleanBankStatement:

    def __init__(
        self,
        logger: Optional[Logger] = None,
        handler: Optional[FileHandler] = None
    ) -> None:

        self.logger = logger or Logger(level=logging.INFO)
        self.handler = handler or FileHandler(logger=self.logger)
        # Configuration: Patterns to mask with generic descriptions
        self.MASKING_MAP = {
            "Transfer": "Transfers",
            "Mobile|Airtime|SMS|USSD": "Phone & Data",
            "Electricity": "Electricity bill",
            "Card|Merchant": "Purchases",
        }

    def _mask_description(
        self,
        df: DataFrame,
        col: str,
        pattern: str,
        generic_desc: str
    ) -> None:
        """
        Replace description patterns with generic labels for privacy/grouping.

        Modifies the DataFrame in-place by masking matching descriptions.

        """
        mask = cast(Series, df[col].str.contains(pattern, na=False))
        df.loc[mask, col] = generic_desc

    def _drop_description(
        self,
        df: DataFrame,
        col: str,
        pattern: str
    ) -> DataFrame:
        """
        Remove transactions with descriptions matching a pattern.

        Returns a new DataFrame with matching rows removed.

        """
        mask = cast(Series, df[col].str.contains(pattern, na=False))
        rows_to_drop = cast(Series, df[mask])
        return df.drop(rows_to_drop.index)

    def clean_data(
        self,
        file_path: str,
        save_path: str = "data/cleaned_bank_statement_2025.parquet",
        rows_to_skip: int = 6
    ) -> None:

        self.logger.info(f"Loading data from {file_path}")
        self.logger.debug(f"Skipping {rows_to_skip} header rows")

        # Load data
        loader = DataLoader(file_path, logger=self.logger)
        bank_st_df = loader.load_excel_single(skip_rows=rows_to_skip)

        self.logger.info(f"Loaded {len(bank_st_df)} transactions")
        self.logger.debug("Starting data cleaning...")

        # Initialize analyzer
        analyzer = DataAnalyzer(logger=self.logger)

        # Normalize column names
        self.logger.debug("Normalizing column names")
        bank_st_df = analyzer.normalize_column_names(bank_st_df)
        bank_st_df.columns = bank_st_df.columns.str.replace(" ", "_")

        # Filter to debit transactions only
        self.logger.debug("Filtering to debit transactions only")
        initial_count = len(bank_st_df)
        bank_st_df = cast(
            DataFrame,
            bank_st_df[bank_st_df["credit(₦)"] == "--"]
        )
        self.logger.debug(
            f"Kept {len(bank_st_df)}/{initial_count} debit transactions")

        # Drop unnecessary columns
        self.logger.debug("Removing unnecessary columns")
        columns_to_drop = [
            "value_date",
            "channel",
            "credit(₦)",
            "balance_after(₦)",
            "transaction_reference",
        ]
        bank_st_df = bank_st_df.drop(columns=columns_to_drop)

        # Rename trans._date column
        bank_st_df = bank_st_df.rename(columns={"trans._date": "trans_date"})

        # Drop specific transaction types
        self.logger.debug("Removing savings and investment transactions")

        # Patterns for transactions to exclude
        # phone_numbers = "8051021438|9058929223|8111016740|9037527321"
        keywords = "Save|OWealth|Fixed"

        before_drop = len(bank_st_df)
        # bank_st_df = drop_description(bank_st_df, "description", phone_numbers)
        bank_st_df = self._drop_description(
            bank_st_df, "description", keywords)
        dropped = before_drop - len(bank_st_df)

        self.logger.debug(f"Removed {dropped} excluded transactions")

        # Mask sensitive/specific descriptions with generic labels
        self.logger.debug("Masking transaction descriptions")
        for pattern, generic_desc in self.MASKING_MAP.items():
            self._mask_description(
                bank_st_df, "description", pattern, generic_desc)

        # Convert data types
        self.logger.debug("Converting data types")
        bank_st_df["trans_date"] = analyzer.convert_datetime(
            cast(Series, bank_st_df["trans_date"]))
        bank_st_df["description"] = bank_st_df["description"].astype(
            "category")
        bank_st_df["debit(₦)"] = analyzer.convert_numeric(
            cast(Series, bank_st_df["debit(₦)"]))

        # Add derived column: transaction month
        self.logger.debug("Adding transaction month column")
        bank_st_df["trans_month"] = bank_st_df["trans_date"].dt.to_period("M")

        # Final validation
        self.logger.debug("Validating cleaned data")
        analyzer.validate_columns_exist(
            bank_st_df,
            ["trans_date", "description", "debit(₦)", "trans_month"]
        )

        self.logger.info(
            f"Cleaning completed: {len(bank_st_df)} transactions retained")
        self.logger.info(
            f"Date range: {bank_st_df['trans_date'].min()} to {bank_st_df['trans_date'].max()}")
        self.logger.info(f"Categories: {bank_st_df['description'].nunique()}")
        #
        save_path = str(self.handler.get_script_dir() / save_path)

        # Save cleaned data
        saver = DataSaver(logger=self.logger)
        saver.save_parquet_compressed(bank_st_df, save_path)
