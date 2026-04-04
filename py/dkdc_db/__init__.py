import sys

from dkdc_db._core import Db, run_cli

__all__ = ["Db", "run_cli"]


def main() -> None:
    """CLI entry point."""
    try:
        run_cli()
    except KeyboardInterrupt:
        sys.exit(130)
