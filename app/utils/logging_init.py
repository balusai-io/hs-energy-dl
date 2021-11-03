import logging


def init_logger():
    """Function to initialize the logger as part of the application"""
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger