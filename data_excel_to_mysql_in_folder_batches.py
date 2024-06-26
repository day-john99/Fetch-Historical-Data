'''

 each time before running this code
 1. in the package add the folder of excel files to be transferred to mysql
 2. in this code update the folder_path variable with name of new folder
 3. update the initial value of current_f_sid in line 29 ,as each f_sid is unique for each excel file everytime
    irrespective of folder
'''

import mysql.connector
import pandas as pd
import os
#from datetime import datetime

# MySQL configuration
config = {
  'user': 'root',
  'password': '**********',
  'host': '127.0.0.1',
  'database': 'stock',
  'raise_on_warnings': True
}

# Folder containing the Excel files
folder_path = 'ADD_YOUR_FOLDER_PATH'


# Initialize f_sid
current_f_sid = 10001


# Define the insert query
insert_query = '''
    INSERT INTO {table_name} (
        d_stockcode, d_exchange, d_products, d_expirydate, d_strikeprice, d_right, d_datetime, d_open, d_high, d_low, d_close, d_volume, d_openinterest, d_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Function to process each Excel file
def process_excel_file(file_path, table_name):



    table_schema = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            z_id INT AUTO_INCREMENT PRIMARY KEY,
            d_datetime DATETIME,
            f_sid INT DEFAULT {current_f_sid},
            d_stockcode VARCHAR(20),
            d_exchange VARCHAR(4),
            d_products VARCHAR(15),
            d_expirydate DATE,
            d_right VARCHAR(5),
            d_strikeprice INT,
            d_open DECIMAL(10,4),
            d_high DECIMAL(10,4),
            d_low DECIMAL(10,4),
            d_close DECIMAL(10,4),
            d_volume INT,
            d_openinterest INT,
            d_count INT,
            FOREIGN KEY (f_sid) REFERENCES strikes_list(sid)

        )
    '''

    try:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(file_path)

        # Modify string type into date and datetime type
        df['expiry_date'] = pd.to_datetime(df['expiry_date']).dt.date
        df['datetime'] = pd.to_datetime(df['datetime'])

        # Connect to MySQL
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # Create the table if it doesn't exist
        cursor.execute(table_schema.format(table_name=table_name))

        # Insert data into the table
        for index, row in df.iterrows():
            cursor.execute(insert_query.format(table_name=table_name), (
                row['stock_code'], row['exchange_code'], row['product_type'], row['expiry_date'], row['strike_price'],
                row['right'], row['datetime'], row['open'], row['high'], row['low'], row['close'], row['volume'],
                row['open_interest'], row['count']
            ))

        # Commit the transaction
        cnx.commit()

        # Close the cursor and connection
        cursor.close()
        cnx.close()

        print(f"Successfully imported {file_path} into table {table_name}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")




# Iterate over all Excel files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        file_path = os.path.join(folder_path, filename)
        table_name = os.path.splitext(filename)[0].replace(' ', '_')  # Sanitize table name
        process_excel_file(file_path, table_name)
        current_f_sid = current_f_sid+1


print("END OF CODE")




