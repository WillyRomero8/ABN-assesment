import logging
from logging.handlers import RotatingFileHandler
import os

# Initiate logger
def initiate_logger(logging_level=None) -> logging.Logger:

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
