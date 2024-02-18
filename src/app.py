import os
import argparse
import pyspark.sql
from function_log import initiate_logger
from pyspark.sql.functions import col


def get_parameters() -> dict:
    """
    Parse command line arguments and return them as a dictionary.

    :return: A dictionary containing parsed command line arguments.
    :rtype: dict
    """
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

def check_parameters(params:dict):
    """
    Check if the required parameters are passed and log relevant information.

    :param params: A dictionary containing parsed command line arguments.
    :type params: dict
    :raises IndexError: If the number of parameters is not equal to 3.
    """
    if len(params) != 3:
        logger.error(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
        raise IndexError(f"You should pass 3 paramaters; path_file_one, path_file_two and nationalities to filter")
    
    if not params['nationalities']:
        logger.warning("The app will not filter any nationality out")
    
    logger.info("The number of paramters is valid.")


def standardize_directory(file_path:str)-> str:
    """
    Standardize the directory path by replacing forward slashes with backslashes
    and logging relevant information.

    :param file_path: The file path to be standardized.
    :type file_path: str
    :return: The standardized file path.
    :rtype: str
    :raises FileNotFoundError: If the specified file path does not exist.
    """
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
    """
    Read a CSV file into a DataFrame and exclude specified columns.

    :param file_path: The path to the input CSV file.
    :type file_path: str
    :param cols_to_exclude: Columns to be excluded from the DataFrame.
    :type cols_to_exclude: tuple
    :return: The DataFrame with specified columns excluded.
    :rtype: pyspark.sql.dataframe.DataFrame
    """
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
    """
    Filter rows of a DataFrame based on a given condition and attribute.

    :param df: The DataFrame to be filtered.
    :type df: pyspark.sql.DataFrame
    :param condition: List of values to filter on.
    :type condition: list
    :param attribute: The attribute/column name to filter on.
    :type attribute: str
    :return: The filtered DataFrame.
    :rtype: pyspark.sql.DataFrame
    """
    try:
        filtered_df = df.filter(col(attribute).isin(condition))
    except Exception as e:
        logger.error(f"An error occurred in df_filter_rows: {str(e)}")
    
    logger.info(f"The output dataset will contain the records of the following nationalitites:{condition}")

    return filtered_df

def df_renamed_columns(df:pyspark.sql.DataFrame, column_mapping:dict) -> pyspark.sql.DataFrame:
    """
    Rename columns in a DataFrame based on the provided mapping.

    :param df: The DataFrame to be modified.
    :type df: pyspark.sql.DataFrame
    :param column_mapping: A dictionary specifying the old and new column names.
    :type column_mapping: dict
    :return: The DataFrame with renamed columns.
    :rtype: pyspark.sql.DataFrame
    """
    renamed_df = df
    for old_name, new_name in column_mapping.items():
        try:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
        except Exception as e:
           logger.error(f"An error occurred in df_renamed_columns: {str(e)}") 

    return renamed_df

def df_to_csv(df:pyspark.sql.DataFrame, output_path:str):
    """
    Write a DataFrame to a CSV file in a specific folder.

    :param df: The DataFrame to be written.
    :type df: pyspark.sql.DataFrame
    :param output_path: The path to save the resulting CSV file.
    :type output_path: str
    """
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

# Mapping to renname original column names {old:new}
col_mapping = {"id":"client_identifier",
               "btc_a":"bitcoin_address",
               "cc_t":"credit_card_type"}


def main():
    
    params = get_parameters()
    check_parameters(params)
    file_path_one = params['dataset_one']
    file_path_two = params['dataset_two']
    file_path_one = standardize_directory(file_path_one)
    file_path_two = standardize_directory(file_path_two)

    # Read CSV file into a DataFrame
    df_1 = df_read_excluding_cols(file_path_one, 'first_name', 'last_name')
    nat_to_f = params["nationalities"]

    if not nat_to_f == None: 
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

