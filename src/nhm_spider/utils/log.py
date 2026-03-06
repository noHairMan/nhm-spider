from logging import getLogger
from typing import Optional


def get_logger(name: Optional[str] = "nhm-spider"):
    return getLogger(name or __name__)
