import logging
import sys

def get_logger(name:str=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # logging.basicConfig(
    #     level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    # Clean previous "handlers"

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s | %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Set rasterio log level
    logger.propagate = False
    return logger