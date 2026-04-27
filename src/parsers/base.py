

# src/parsers/base.py

from typing import cast
from haashi_pkg.data_engine import DataSaver, DataFrame, Series
from haashi_pkg.utility import Logger, FileHandler


class BaseParser:

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

    def _save_data(
        self,
        df: DataFrame,
        save_path: str,
        logger: Logger,
        handler: FileHandler
    ) -> None:
        save_path = str(handler.get_script_dir() / save_path)

        # Save cleaned data
        saver = DataSaver(logger=logger)
        saver.save_parquet_compressed(df, save_path)
