"""(c) 2025, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

import logging


class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to logging levels"""

    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + fmt + self.reset,
            logging.INFO: self.blue + fmt + self.reset,
            logging.WARNING: self.yellow + fmt + self.reset,
            logging.ERROR: self.red + fmt + self.reset,
            logging.CRITICAL: self.bold_red + fmt + self.reset,
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logger(name: str) -> logging.Logger:
    """Set up and return a configured logger instance."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create console handler with custom formatter
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create custom format with emojis
    log_format = "%(asctime)s 🕒 [%(levelname)s] %(message)s"
    ch.setFormatter(ColoredFormatter(log_format))
    logger.addHandler(ch)

    return logger
