import logging
import sys


def setup_logging(verbosity: int = 0) -> None:
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbosity, len(levels) - 1)]

    logging.basicConfig(
        level=level,
        format="%(levelname)s | %(message)s",
        stream=sys.stderr,
        force=True,
    )
