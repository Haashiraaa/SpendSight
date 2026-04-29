

# src/main.py

import logging
from src.helpers.cli import parse_args
from src.parsers.opay import OpayParser
from haashi_pkg.utility import Logger


def main(logger: Logger = Logger(logging.INFO)) -> None:

    args = parse_args()

    if args.debug:
        logger = Logger(logging.DEBUG)

    op = OpayParser(logger=logger)
    op.parse_data(args.file)


if __name__ == "__main__":
    main()
