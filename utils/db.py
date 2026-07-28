import os


def get_database_url() -> str:
    """
    Return the PostgreSQL connection string.

    Returns:
        The PostgreSQL connection URL from the DATABASE_URL environment variable.

    Raises:
        RuntimeError: if DATABASE_URL is not set.
    """

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Set it explicitly — e.g. "
            "postgresql://retail_user:retail_password@localhost:5432/retail_source "
            "for local Docker, or the Azure PostgreSQL connection string for the cloud instance."
        )
    return database_url
