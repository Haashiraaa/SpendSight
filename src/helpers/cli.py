

# src/helpers/cli.py

import argparse


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="Bank statement file path"
    )

    parser.add_argument(
        "--bank",
        type=str,
        default="Opay",
        help="Name of Bank"

    )

    parser.add_argument(
        "-d",
        "--debug",
        action='store_true',
        help='Enable debug logging'
    )

    return parser.parse_args()
