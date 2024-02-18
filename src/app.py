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

def standardize_directory(file_path:str)-> str:
 
    if file_path.startswith('/') or file_path.startswith('\\'):
        file_path = file_path[1:]
    
    file_path = file_path.replace('/', '\\')

    if os.path.exists(file_path):
        logger.info(f"The path {file_path} exists")
    else:
        logger.error(f"The path {file_path} does not exists")
        raise FileNotFoundError(f"The path {file_path} does not exists")
    
    return file_path


def df_read_excluding_cols(file_path:str, *cols_to_exclude)-> pyspark.sql.dataframe.DataFrame:

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        logger.error(f"An error occurred in df_read_excluding_cols: {str(e)}")
    

    # Check if all tuple values are present in the list
    not_present_col = [col for col in cols_to_exclude if col not in df.columns]

    # Warning
    if not_present_col:
        logger.warning(f"Not all the given columns to exclude are actual columns in the dataframe; the app will omit all those cases: {not_present_col}")
    
    try:
        filtered_df = df.drop(*cols_to_exclude)
    except Exception as e:
        logger.error(f"An error occurred in df_read_excluding_cols: {str(e)}")

    return filtered_df

def df_filter_rows(df:pyspark.sql.DataFrame, condition:list, attribute:str)-> pyspark.sql.DataFrame:
    try:
        filtered_df = df.filter(col(attribute).isin(condition))
    except Exception as e:
        logger.error(f"An error occurred in df_filter_rows: {str(e)}")

    return filtered_df

def df_renamed_columns(df:pyspark.sql.DataFrame, column_mapping:dict) -> pyspark.sql.DataFrame:

    renamed_df = df
    for old_name, new_name in column_mapping.items():
        try:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
        except Exception as e:
           logger.error(f"An error occurred in df_renamed_columns: {str(e)}") 

    return renamed_df

def df_to_csv(df:pyspark.sql.DataFrame, output_path:str):

    # Setting as a variable the root directory
    cwd = os.getcwd()
    rd = os.path.abspath(os.path.join(cwd, os.pardir))

    # Creating the directory to write the resulting .csv files
    output_path = os.path.join(rd, output_path)

    try:
        # Save the DataFrame as a CSV file
        df.coalesce(1).write.mode('overwrite').option("header", "true").csv(output_path)
    except Exception as e:
         print(f"An error occurred when trying to write the output dataframe: {str(e)}")

    
logger = initiate_logger(10) 
spark = pyspark.sql.SparkSession.builder.appName("abn-assesment").getOrCreate()
# Specify the path where you want to save the CSV file
output_file_path = "client_data/result.csv"


col_mapping = {"id":"client_identifier",
               "btc_a":"bitcoin_address",
               "cc_t":"credit_card_type"}


def main():
    
    params = get_parameters()
    check_paramaters(params)
    file_path_one = params['dataset_one']
    file_path_two = params['dataset_two']
    file_path_one = standardize_directory(file_path_one)
    file_path_two = standardize_directory(file_path_two)

    # Read CSV file into a DataFrame
    df_1 = df_read_excluding_cols(file_path_one, 'first_name', 'last_name')
    nat_to_f = params["nationalities"]
    df_1 = df_filter_rows(df_1, nat_to_f, 'country')
    df_2 = df_read_excluding_cols(file_path_two, 'cc_n')
    try:
        joined_df = df_1.join(df_2, on="id", how="inner")
    except Exception as e:
        logger.error(f"An error occurred when trying to join both dataframes: {str(e)}")
    rn_df = df_renamed_columns(joined_df, col_mapping)
    
    df_to_csv(rn_df, 'client_data')

    spark.stop()

if __name__ == '__main__':
    main()

