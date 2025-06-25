import logging

import colorlog


def setup_logger():
    root_logger = logging.getLogger()

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )
    root_logger.addHandler(handler)


def set_log_level(level):
    logging.getLogger().setLevel(level)


def get_logger(name):
    return logging.getLogger(name)
