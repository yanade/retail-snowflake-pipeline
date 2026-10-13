"""Start-up helpers shared by the generator scripts: logging and the database URL."""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv


def setup_logging() -> logging.Logger:
    """
    Configure and return a shared logger.

    Duplicated from utils/logger.py rather than imported — this script is
    run as `cd database && python generate_data.py`, not
    `python -m database.generate_data` from the project root, so `utils`
    isn't importable here.

    Returns:
        Configured root logger.
    """

    if logging.getLogger().handlers:
        return logging.getLogger()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger()


def get_database_url() -> str:
    """
    Return the PostgreSQL connection string.

    Returns:
        The PostgreSQL connection URL from the DATABASE_URL environment variable.

    Raises:
        RuntimeError: if DATABASE_URL is not set.
    """

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Set it explicitly — e.g. "
            "postgresql://retail_user:retail_password@localhost:5432/retail_source "
            "for local Docker, or the Azure PostgreSQL connection string for the cloud instance."
        )
    return database_url

