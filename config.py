# table schemas
schema_users = '''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    signup_date TEXT,
    country TEXT
);
'''

schema_transactions = '''
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    transaction_date TEXT,
    amount REAL,
    transaction_type TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);
'''

# data types
users_expected_dtypes = {'user_id': 'int64', 'signup_date': 'object', 'country': 'object'}
transactions_expected_dtypes = {'transaction_id': 'int64', 'user_id': 'int64', 'transaction_date': 'object', 'amount': 'float64', 'transaction_type': 'object'}
