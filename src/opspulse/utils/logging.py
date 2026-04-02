from __future__ import annotations

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure a consistent application logger for local ETL runs."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger used by the ETL pipeline."""
    return logging.getLogger(name)
