import sqlite3
import pandas as pd
import logging
from config import (
    schema_users, 
    schema_transactions,
    users_expected_dtypes,
    transactions_expected_dtypes
)


logging.basicConfig(
    filename="app.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S" 
)
logger = logging.getLogger()


def clean_data(df: pd.DataFrame, table_name: str, dataypes: dict) -> pd.DataFrame:
    """Clean data by handling missing values, duplicates and validate data types."""
    # check for missing values
    if df.isnull().values.any():
        logger.warning(f'Warning: Missing values found in {table_name}. Filling with defaults.')
        df.fillna({
            'user_id': -1,
            'amount': 0.0,
            'transaction_type': 'unknown'
        }, inplace=True)

    # check for duplicates
    if df.duplicated().any():
        logger.warning(f'Warning: Duplicate rows found in {table_name}. Removing duplicates.')
        df.drop_duplicates(inplace=True)

    # validate data types match expected data types
    for column, dtype in df.dtypes.items():
        if dtype != dataypes[column]:
            logger.warning(f'Error: Unexpected data type for column {column} in {table_name}.')
            raise ValueError(f'Invalid data type in {table_name}: {column} is {dtype}')

    return df


def ingest_data(db_file, file_name, schema, table_name, dataypes):

    # read data from CSV file
    df = pd.read_csv(file_name)

    # clean data and validate quality
    df_clean = clean_data(df, table_name, dataypes)
    
    # create table in database
    try:
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        cursor.execute(schema)
        df_clean.to_sql(table_name, connection, if_exists='replace', index=False)

        logger.info(f'Successfully ingested table {table_name} in database.')
    except Exception as e:
        logger.error(f'Error encountered during ingestion of table {table_name} in database: {e}')
    finally:
        connection.close()

# define file names to be ingested
csv_file_1 = 'users.csv'
csv_file_2 = 'transactions.csv'
db_file = 'traffic.db'

# ingest clean data into database
ingest_data(db_file, csv_file_1, schema_users, 'users', users_expected_dtypes)
ingest_data(db_file, csv_file_2, schema_transactions, 'transactions', transactions_expected_dtypes)
