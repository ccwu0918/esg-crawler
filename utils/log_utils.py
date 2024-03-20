import os
import logging
import sys
from datetime import datetime
from pathlib import Path


def log_setting(
    log_folder: str = f"logs-default", log_level: int = logging.INFO, stream: bool = True
):
    
    log_folder = log_folder if log_folder.startswith("logs-") else "logs-" + log_folder
    log_filepath = os.path.join(
        Path(__file__).resolve().parent,
        "../"+log_folder,
        f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log",
    )
    Path(log_filepath).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(name)s | line:%(lineno)s | %(funcName)s] [%(levelname)s] - %(message)s"
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    return logger


logger = log_setting()
