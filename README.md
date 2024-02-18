# ABN-assesment

This application is designed to process csv files and generate an output by:
    - Filtering personal data from both datasets. 
    - Filtering the data based on the specific nationalaties
    - Joining the dataset on 'id' key

## Features

- Input: Two datasets in CSV format in src/raw_data/
    - Expected colummns in dataset_one:
        -id -> int
        -first_name -> string
        -last_name -> string
        -email -> string
        -country -> string
    - Expected colummns in dataset_two:
        -id -> int
        -btc_a -> string
        -cc_t -> string
        -cc_n -> int
- Output: Filtered data based on specified criteria in /client_data/
    - Expected colummns in outpur dataset:
        -client_identifier -> int
        -email -> string
        -country -> string
        -bitcoin_address -> string 
        -credit_card_type -> string

## Requirements

- Python 3.8.10
- pyspark library (3.5.0)
- py4j library (0.10.9.7)

## Installation

1. Clone this repository to your local machine: 
    ```git clone <url> ```
2. Navigate to the project directory
3. Install the required dependencies:
    ```pip install -r requirements.txt```

## Usage

1. Place your input datasets in the src/raw_data directory
2. Edit the `app.py` script to specify the filtering criteria, columns to exclude or join condition
3. Navigate to src folder where app.py is located 
4. Run the script:
    ```python app.py <path_file1> <path_file2> --nationalities "Nat1" "Nat2" ```

> [!NOTE]
> Whereas path_file1 and path_file2 are mandatory parameters, the list of nationalitites is not.
> Nonetheless the application will not filter by any nationality and there will be a warning log  
