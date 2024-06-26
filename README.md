# Stock Data Fetching and Processing

This repository contains Python scripts to fetch stock market data using BreezeConnect, process the data, and store it in a MySQL database. The scripts are divided into three main files:

1. `spot2fetch.py`: Fetches spot data for a specific stock.
2. `options2fetch.py`: Fetches options data for a specific stock.
3. `data_excel_to_mysql_in_folder_batches.py`: Processes Excel files and stores the data into a MySQL database.

## Prerequisites

- Python 3.x
- Pandas
- OpenPyXL
- BreezeConnect SDK
- MySQL Server

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/your-repo.git
    cd your-repo
    ```

2. Install the required Python packages:
    ```bash
    pip install pandas openpyxl mysql-connector-python breeze-connect
    ```

3. Set up your MySQL database and create the necessary tables.

## Usage

### 1. spot2fetch.py

This script fetches historical spot data for a specific stock from the BreezeConnect API and saves it into an Excel file.

1. Update the BreezeConnect API credentials in the script:
    ```python
   api_key="BREEZE_API_KEY"
   api_secret="BREEZE_API_SECRET"
   session_token="SESSION_TOKEN"
    ```

2. Run the script:
    ```bash
    python spot2fetch.py
    ```

### 2. options2fetch.py

This script fetches historical options data for a specific stock from the BreezeConnect API and saves it into multiple Excel files.

1. Update the BreezeConnect API credentials in the script:
    ```python
   api_key="BREEZE_API_KEY"
   api_secret="BREEZE_API_SECRET"
   session_token="SESSION_TOKEN"
    ```

2. Update the expiry date and other parameters as required.

3. Run the script:
    ```bash
    python options2fetch.py
    ```

### 3. data_excel_to_mysql_in_folder_batches.py

This script processes multiple Excel files in a folder and stores the data into a MySQL database.

1. Update the MySQL configuration in the script:
    ```python
   config = {
      'user': 'root',
      'password': 'YOUR_PASSWORD',
      'host': '127.0.0.1',
      'database': 'stock',
      'raise_on_warnings': True
    }
    ```

2. Update the `folder_path` variable with the path to the folder containing the Excel files.

3. Set the initial value of `current_f_sid` to a unique value for each run.

4. Run the script:
    ```bash
    python data_excel_to_mysql_in_folder_batches.py
    ```

## Notes

- Ensure that the MySQL database and tables are properly set up before running the scripts.
- Handle API limits and errors as per your requirements.
- Modify the scripts to suit your specific use case.


