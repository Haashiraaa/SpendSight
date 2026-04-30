

# src/main.py

import logging
from src.helpers.cli import parse_args
from src.parsers.opay import OpayParser
from src.core.analysis import FinanceAnalysis
from haashi_pkg.utility import Logger
from src.core.visualization import FinanceDashboard


def main(logger: Logger = Logger(logging.INFO)) -> None:

    args = parse_args()

    if args.debug:
        logger = Logger(logging.DEBUG)

    op = OpayParser(logger=logger)
    debit_df, credit_df = op.parse_data(args.file)

    fa = FinanceAnalysis(debit_df, credit_df, logger)

    dash_fi = FinanceDashboard(fa, logger=logger)
    dash_fi.visualize_data()


if __name__ == "__main__":
    main()
