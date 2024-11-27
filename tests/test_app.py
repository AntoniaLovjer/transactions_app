import sys
sys.path.append('./')

from app import app
import unittest

class TestFlaskAPIs(unittest.TestCase):
    def setUp(self):
        """Set up the Flask test client."""
        self.app = app.test_client()
        self.app.testing = True

    def test_get_transaction_totals(self):
        """Test the /api/summary/transaction_totals endpoint."""
        response = self.app.get('/api/summary/transaction_totals')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('user_id', data[0])
        self.assertIn('total_transaction_amount', data[0])
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)

    def test_get_top_users(self):
        """Test the /api/summary/top_users endpoint."""
        response = self.app.get('/api/summary/top_users')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('user_id', data[0])
        self.assertIsInstance(data, list)
        self.assertTrue(len(data) <= 10)
    
    def test_get_daily_transaction_totals(self):
        """Test the /api/summary/daily_transaction_totals endpoint."""
        response = self.app.get('/api/summary/daily_transaction_totals')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('transaction_date', data[0])
        self.assertIn('transaction_count', data[0])
        self.assertIn('transaction_amount', data[0])
        self.assertIsInstance(data, list)

    def test_invalid_endpoint_name(self):
        """Test a non-existent endpoint."""
        response = self.app.get('/api/summary/nonexistent')
        self.assertEqual(response.status_code, 404)
    
    def test_invalid_endpoint_type(self):
        """Test a non-valid endpoint type."""
        response = self.app.post('/api/summary/top_users')
        self.assertEqual(response.status_code, 500)
