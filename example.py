import pandas as pd
# df_transactions = pd.DataFrame(data=[
#     ['2024-01-01',1, 4],
#     ['2024-01-01', 2, 10],
#     ['2024-02-01', 3, 5]]
#     , columns=['transaction_date', 'transaction_id', 'amount'])

# df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
# daily_totals = df_transactions.groupby(df_transactions['transaction_date'].dt.date).agg({
#     'transaction_id': 'count',
#     'amount': 'sum'
# }).reset_index()
# daily_totals.columns = ['transaction_date', 'transaction_count', 'transaction_amount']

# print(daily_totals)

# df = pd.DataFrame({
#     'A': [1, 2],
#     'B': ['x', 'y'],
#     'C': [1.5, 2.5]
# })

# # Get the data types of each column
# print(df.dtypes)

# expected_dtypes = {'A': 'int64', 'B': 'object', 'C': 'object'}

# # Check if dtypes match
# dtypes_match = df.dtypes.to_dict() == expected_dtypes

# print(dtypes_match)

import sqlite3
from flask import Flask, request, jsonify, make_response
import pandas


def get_db_connection():
    """
    Helper function for database connection
    """
    connection = sqlite3.connect('traffic.db')
    connection.row_factory = sqlite3.Row
    return connection

connection = get_db_connection()
result = connection.execute("SELECT * FROM TopUsers").fetchall()
connection.close()
summary = [
    {
        'user_id': row['user_id'],
        'total_transaction_amount': row['total_transaction_amount']
    } for row in result
]
print(pd.DataFrame(summary))

# result = [(1, '2022-11-23', 'Japan'), (2, '2022-11-23', 'France')]
# summary = [
#             {
#             'user_id': row['user_id'], 
#             'signup_date': row['signup_date'], 
#             'country': row['country']
#             } for row in result
#         ]
# print(summary)

from api import app
response = app.test_client().app.get('/api/summary/top_users')
data = response.get_json()
print(len(data['user_id']))