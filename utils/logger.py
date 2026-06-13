import logging

# ------------------------------------
# Logging configuration
# ------------------------------------

def setup_logging() -> logging.Logger:

    if logging.getLogger().handlers:
        return logging.getLogger()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    return logging.getLogger()