import pytest
import chispa
from pyspark.sql import Row
from app import *



def test_check_parameters():
    # Test when all parameters are provided
    params = {'dataset_one': 'file1.csv', 'dataset_two': 'file2.csv', 'nationalities': ['Netherlands', 'United Kingdom']}
    assert check_parameters(params) is None

    # Test when 'nationalities' parameter is not provided
    # This is the case when no nationialites are passed as input parameters
    params = {'dataset_one': 'file1.csv', 'dataset_two': 'file2.csv', 'nationalities': None}
    assert check_parameters(params) is None

    # Test when 'nationalities' parameter is an empty list
    params = {'dataset_one': 'file1.csv', 'dataset_two': 'file2.csv'}
    with pytest.raises(IndexError):
        check_parameters(params)

def test_standardize_directory():
    # Test when the input path starts with '/'
    input_path = '/raw_data/dataset_one.csv'
    expected_output = 'raw_data/dataset_one.csv'
    assert standardize_directory(input_path) == expected_output

    # Test when the input path doesn't start with '/'
    input_path = 'raw_data/dataset_one.csv'
    assert standardize_directory(input_path) == expected_output

    # Test when the directory doesn't exist
    input_path = 'nonexistent\\directory'
    with pytest.raises(FileNotFoundError):
        standardize_directory(input_path)


def test_df_read_excluding_cols():
    
    #Test when there are no columns to exclude are properly set
    file_path = "raw_data/test_dataset.csv"

    expected_df = spark.read.csv(file_path, header=True, inferSchema=True)

    result_df = df_read_excluding_cols(file_path)

    chispa.assert_df_equality(result_df, expected_df)

    #Test when the columns to exclude are not properly set
    cols_to_exclude = ('fake_column')

    result_df = df_read_excluding_cols(file_path, *cols_to_exclude)

    chispa.assert_df_equality(result_df, expected_df)

    #Test when the columns to exclude are properly set
    cols_to_exclude = ('first_name', 'last_name')

    expected_df = spark.read.csv(file_path, header=True, inferSchema=True)
    expected_df = expected_df.drop(*cols_to_exclude)

    result_df = df_read_excluding_cols(file_path, *cols_to_exclude)

    chispa.assert_df_equality(result_df, expected_df)


def test_df_filter_rows():
    # Scenario where filters are applied well
    df = spark.createDataFrame([(1, 'USA'), (2, 'UK'), (3, 'Canada')], ["id", "country"])
    condition = ["USA", "UK"]
    attribute = "country"

    # Execution
    result_df = df_filter_rows(df, condition, attribute)

    # Assertion
    assert result_df.count() == 2
    assert result_df.where("country = 'USA'").count() == 1
    assert result_df.where("country = 'UK'").count() == 1
    assert result_df.where("country = 'Canada'").count() == 0

    # Applying a filter with no further action
    condition = ["Spain"]

    # Execution
    result_df = df_filter_rows(df, condition, attribute)

    # Assertion
    assert result_df.count() == 0

    # Attempt to filter by a nonexistent column
    with pytest.raises(Exception) as e_info:
        result_df = df_filter_rows(df, condition, 'fake_colummn')

    # Check if the error message contains the expected substring
    assert "cannot resolve '`nonexistent_column`' given input columns" in str(e_info.value)

def test_df_renamed_columns():
    # Scenario where columns mapping is accurate
    df = spark.createDataFrame([(1, 'USA'), (2, 'UK'), (3, 'Canada')], ["id", "country"])
    # Define column mapping
    column_mapping = {"id": "id_country", "country": "nationality"}

    # Apply column renaming
    result_df = df_renamed_columns(df, column_mapping)
    expected_df = spark.createDataFrame([(1, 'USA'), (2, 'UK'), (3, 'Canada')], ["id_country", "nationality"])
    chispa.assert_df_equality(result_df, expected_df)


    # Scenario where columns mapping is not accurate
    # Define column mapping
    column_mapping_w = {"id_fake": "id_country"}

    # Attempt to rename a nonexistent column
    with pytest.raises(Exception) as e_info:
        result_df = df_renamed_columns(df, column_mapping_w)

    # Check if the error message contains the expected substring
    assert "cannot resolve 'id_fake' given input columns" in str(e_info.value)



if __name__ == '__main__':
    spark = pyspark.sql.SparkSession.builder.appName("test-abn-assesment").getOrCreate()
    test_check_parameters()
    test_standardize_directory()
    test_df_read_excluding_cols()
    test_df_filter_rows()
    test_df_renamed_columns()
    spark.stop()
    



