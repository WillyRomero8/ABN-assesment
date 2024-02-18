import os
import logging
import argparse
import pyspark.sql
from logging.handlers import RotatingFileHandler
from pyspark.sql.functions import col


def get_parameters() -> dict:

    parser=argparse.ArgumentParser()

    parser.add_argument(
        'dataset_one', 
        help='Path to the input file 1, mandatory positional argument')

    parser.add_argument(
        'dataset_two',
        help='Path to the input file 2, mandatory positional argument')

    parser.add_argument(
        "--nationalities",
        nargs="*", 
        type=str,
        help='List of nationalities to filter the output data'
    )

    parameters = vars(parser.parse_args())
 
    return parameters

def check_paramaters(params:dict):
    if len(params) != 3:
        logger.error(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
        raise IndexError(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
    
    if not params['nationalities']:
        logger.warning("The app will not filter any nationality out")
    
    logger.info("The number of paramters is valid.")

# Initiate logger
def initiate_logger(logging_level=None) -> logging.Logger:

    # Create a logs directory if it doesn't exist
    logs_dir = 'logs'
    os.makedirs(logs_dir, exist_ok=True)

    # Configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

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

def check_directory(file_path:str):
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


def df_read_excluding_cols(file_path:str, *cols_to_exclude)-> pyspark.sql.dataframe.DataFrame:

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        logger.error(f"An error occurred in df_read_excluding_cols: {str(e)}")

    # Check if all tuple values are present in the list
    not_present_col = [col for col in cols_to_exclude if col not in df.columns]

    # Print the result
    if not_present_col:
        logger.warning(f"Not all the given columns to exclude are actual columns in the dataframe; the app will omit all those cases: {not_present_col}")
    
    try:
        filtered_df = df.drop(*cols_to_exclude)
    except Exception as e:
        logger.error(f"An error occurred in df_read_excluding_cols: {str(e)}")

    return filtered_df

def df_filter_rows(df:pyspark.sql.DataFrame, condition:list, attribute:str)-> pyspark.sql.dataframe.DataFrame:

    try:
        filtered_df = df.filter(col(attribute).isin(condition))
    except Exception as e:
        logger.error(f"An error occurred in df_filter_rows: {str(e)}")

    return filtered_df


    
logger = initiate_logger(10) 
spark = pyspark.sql.SparkSession.builder.appName("abn-assesment").getOrCreate()

def main():
    
    params = get_parameters()
    check_paramaters(params)
    file_path_one = params['dataset_one']
    file_path_two = params['dataset_two']
    check_directory(file_path_one)
    check_directory(file_path_two)

    # Read CSV file into a DataFrame
    df_1 = df_read_excluding_cols(file_path_one, 'first_name', 'last_name')
    nat_to_f = params["nationalities"]
    df_1 = df_filter_rows(df_1, nat_to_f, 'country')
    df_2 = df_read_excluding_cols(file_path_two, 'cc_n')


    spark.stop()

if __name__ == '__main__':
    main()

