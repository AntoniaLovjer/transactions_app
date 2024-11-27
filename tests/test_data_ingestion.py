import sys
sys.path.append('./')

import unittest
import sqlite3
import pandas as pd
from pandas.testing import assert_frame_equal
from data_ingestion import clean_data, ingest_data
from config import (
    schema_users, 
    schema_transactions,
    users_expected_dtypes,
    transactions_expected_dtypes
)

class TestCleanData(unittest.TestCase):
    """Test class for raw data cleaning function."""
    @classmethod
    def setUp(self):
        """Set up sample data for testing."""
        self.sample_data = pd.DataFrame({
            'transaction_id': [1, 2, 3, 3],
            'amount': [100.0, 200.0, 300.0, 300.0],
            'transaction_type': ['purchase', None, 'deposit', 'deposit']
        })
        self.expected_dtypes = {
            'transaction_id': 'int64',
            'amount': 'float64',
            'transaction_type': 'object'
        }

    def test_missing_values(self):
        """Test that missing values are correctly imputed."""
        clean_df = clean_data(self.sample_data.copy(), 'test_table', self.expected_dtypes)
        self.assertEqual(clean_df['transaction_type'].iloc[1], 'unknown')

    def test_duplicates(self):
        """Test that duplicate rows are removed."""
        clean_df = clean_data(self.sample_data.copy(), 'test_table', self.expected_dtypes)
        self.assertEqual(len(clean_df), 3)

    def test_data_type_validation(self):
        """Test that invalid data types raise a ValueError."""
        invalid_data = self.sample_data.copy()
        invalid_data['transaction_id'] = invalid_data['transaction_id'].astype('object')
        with self.assertRaises(ValueError):
            clean_data(invalid_data, 'test_table', self.expected_dtypes)


class TestIngestData(unittest.TestCase):
    """Test class for data ingestion function."""
    @classmethod
    def setUp(self):
        self.db_file = 'test.db'
        self.schema_users = schema_users
        self.schema_transactions = schema_transactions
        self.sample_users_data = pd.DataFrame([
            {'user_id': 1, 'signup_date': '2022-11-23', 'country': 'Japan'},
            {'user_id': 2, 'signup_date': '2022-11-23', 'country': 'France'},
        ], columns=['user_id', 'signup_date', 'country'])
        self.sample_transactions_data = pd.DataFrame([
            {'transaction_id': 1001, 'user_id': 1, 'transaction_date': '2022-01-09', 'amount': 100.0,'transaction_type': 'deposit'},
            {'transaction_id': 1002, 'user_id': 2, 'transaction_date': '2022-03-22', 'amount': 120.10, 'transaction_type': 'purchase'},
        ])
        self.sample_users_file = 'test_users.csv'
        self.sample_transactions_file = 'test_transactions.csv'
        self.users_expected_dtypes = users_expected_dtypes
        self.transactions_expected_dtypes = transactions_expected_dtypes
        self.sample_users_data.to_csv(self.sample_users_file, index=False)
        self.sample_transactions_data.to_csv(self.sample_transactions_file, index=False)

    def test_successful_users_ingestion(self):
        """Test that users data is correctly ingested into the database."""
        import os
        ingest_data(self.db_file, self.sample_users_file, self.schema_users, 'users', self.users_expected_dtypes)
        
        # check table contents
        connection = sqlite3.connect(self.db_file)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        result = cursor.execute('SELECT * FROM users').fetchall()
        summary = [
            {
                'user_id': row['user_id'], 
                'signup_date': row['signup_date'], 
                'country': row['country']
            } for row in result
        ]
        self.assertEqual(len(summary), 2)
        assert_frame_equal(pd.DataFrame(summary), self.sample_users_data)

        connection.close()
        # clean up database file
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
    
    def test_successful_transactions_ingestion(self):
        """Test that transactions data is correctly ingested into the database."""
        import os
        ingest_data(self.db_file, self.sample_transactions_file, self.schema_transactions, 'transactions', self.transactions_expected_dtypes)
        
        # check table contents
        connection = sqlite3.connect(self.db_file)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        result = cursor.execute('SELECT * FROM transactions').fetchall()
        summary = [
            {
                'transaction_id': row['transaction_id'], 
                'user_id': row['user_id'], 
                'transaction_date': row['transaction_date'],
                'amount': row['amount'], 
                'transaction_type': row['transaction_type']
            } for row in result
        ]
        self.assertEqual(len(summary), 2)
        assert_frame_equal(pd.DataFrame(summary), self.sample_transactions_data)
        
        connection.close()
        # clean up database file
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
    
    def test_invalid_users_schema(self):
        """Test that invalid schema raises an error."""
        invalid_schema = """
            CREATE TABLE invalid_table (
                invalid_column TEXT
            )
        """
        with self.assertRaises(Exception):
            ingest_data(self.db_file, self.sample_file, invalid_schema, 'invalid_table', self.users_expected_dtypes)

    @classmethod
    def tearDown(self):
        """Clean up temporary files."""
        import os
        if os.path.exists(self.sample_users_file):
            os.remove(self.sample_users_file)
        if os.path.exists(self.sample_transactions_file):
            os.remove(self.sample_transactions_file)