import sys
from pathlib import Path
from loguru import logger


def setup_logger(log_file: str = "logs/dbma.log", level: str = "INFO"):
    logger.remove()

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_file,
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        level=level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {module}:{function}:{line} | {message}",
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )

    logger.add(
        sys.stderr,
        level="CRITICAL",
        format="{time:HH:mm:ss} | {level} | {message}",
    )

    logger.info("DBMA Logger initialized")
    return logger