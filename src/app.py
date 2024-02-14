import os
import logging
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("abn-assesment").getOrCreate()
spark.stop()

arguments = sys.argv




def get_paramaters(params_list):
    if len(params_list) != 3:
        logger.error(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
        raise IndexError(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
    
    param1, param2, param3 = params_list
    logger.info("The number of paramters is valid.")
 
    return param1,param2,param3

# Initiate logger
def initiate_logger(logging_level=None):

    logger_name = "abn-assesment"
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(logger_name)
    
    if logging_level:
        level = logging_level
    else:
        level = logging.INFO
    
    logger.setLevel(level)

    return logger




def check_directory(file_path):
    # Construct the absolute path by joining with the current working directory
    #print(file_path)
  

    if file_path.startswith('/') or file_path.startswith('\\'):
        file_path = file_path[1:]
    
    file_path = file_path.replace('/', '\\')

    if os.path.exists(file_path):
        logger.info(f"The path {file_path} exists")
    else:
        logger.error(f"The path {file_path} does not exists")
        raise FileNotFoundError(f"The path {file_path} does not exists")
    

 

logger = initiate_logger(10)
file_path_one, file_path_two, list_of_nat = get_paramaters(arguments[1:])
print(file_path_one, file_path_two, list_of_nat)
check_directory(file_path_one)
check_directory(file_path_two)

