import logging 

def logging_config():
    """
    sets the logging config 
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    log_format = "%(asctime)s: %(levelname)s: %(message)s"
    time_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, time_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)