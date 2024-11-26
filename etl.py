import sqlite3
import pandas as pd
import logging

logging.basicConfig(
    filename="app.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S" 
)
logger = logging.getLogger()

logger.info("Starting ETL process...")

# extract data from database and laod into Pandas DataFrames
try:
    connection = sqlite3.connect('traffic.db')
    df_users = pd.read_sql_query("SELECT * FROM users", connection)
except Exception as e:
    logger.error(f"Error encountered during extraction of table 'users' from database: {e}")
finally:
    connection.close()

try:
    connection = sqlite3.connect('traffic.db')
    df_transactions = pd.read_sql_query("SELECT * FROM transactions", connection)
except Exception as e:
    logger.error(f"Error encountered during extraction of table 'transactions' from database: {e}")
finally:
    connection.close()

# calculate total transaction amount per user
user_totals = df_transactions.groupby('user_id')['amount'].sum().reset_index()
user_totals.columns = ['user_id', 'total_transaction_amount']

# get top users
top_users = user_totals.nlargest(10, 'total_transaction_amount')

# aggregate daily transaction totals across all users
df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date']) 
daily_totals = df_transactions.groupby(df_transactions['transaction_date'].dt.date).agg({
    'transaction_id': 'count',
    'amount': 'sum'
}).reset_index()
daily_totals.columns = ['transaction_date', 'transaction_count', 'transaction_amount']

# load data back into the database
try:
    connection = sqlite3.connect('traffic.db')
    user_totals.to_sql('UserTransactionTotals', connection, if_exists='replace', index=False)
    logger.info(f'Successfully ingested processed UserTransactionTotals into database.')
except Exception as e:
    logger.error(f"Error encountered during ingestion of UserTransactionTotals in database: {e}")
finally:
    connection.close()

try:
    connection = sqlite3.connect('traffic.db')
    top_users.to_sql('TopUsers', connection, if_exists='replace', index=False)
    logger.info(f'Successfully ingested processed TopUsers into database.')
except Exception as e:
    logger.error(f"Error encountered during ingestion of TopUsers in database: {e}")
finally:
    connection.close()

try:
    connection = sqlite3.connect('traffic.db')
    daily_totals.to_sql('DailyTransactionTotals', connection, if_exists='replace', index=False)
    logger.info(f'Successfully ingested processed DailyTransactionTotals into database.')
except Exception as e:
    logger.error(f"Error encountered during ingestion of DailyTransactionTotals in database: {e}")
finally:
    connection.close()

logger.info("ETL process complete.")
