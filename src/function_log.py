import logging
from logging.handlers import RotatingFileHandler
import os


# Logging level: numeric value
# NOTSET:0
# DEBUG:10
# INFO:20
# WARNING:30
# ERROR:40
# CRITICAL:50

def initiate_logger(logging_level=None) -> logging.Logger:
    """
    Initialize and configure a logger for the application.

    :param logging_level: The logging level to be set for the logger.
    :type logging_level: int, optional
    :return: The configured logger object.
    :rtype: logging.Logger
    """
    # Create a logs directory if it doesn't exist
    logs_dir = 'logs'
    os.makedirs(logs_dir, exist_ok=True)

    # Create a rotating file handler in the logs directory
    log_file_path = os.path.join(logs_dir, 'app.log')

    logging.basicConfig(
        handlers=[RotatingFileHandler(log_file_path, maxBytes=10_000, backupCount=2)],
        #filename = 'logs/app.log',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO)
    logger = logging.getLogger("abn-assesment")
    
    if logging_level:
        level = logging_level
    else:
        level = logging.INFO
    
    logger.setLevel(level)

    return logger
