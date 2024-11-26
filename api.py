from flask import Flask, request, jsonify, make_response, g
import sqlite3
import logging
import time

logging.basicConfig(
    filename="app.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S" 
)
logger = logging.getLogger()

def get_db_connection():
    """
    Helper function for database connection
    """
    connection = sqlite3.connect('traffic.db')
    connection.row_factory = sqlite3.Row
    return connection

app = Flask(__name__)

# set up logging requests and responses
@app.before_request
def log_request():
    """Log incoming request details."""
    logger.info(f"Request: {request.method} {request.path}")

@app.before_request
def start_timer():
    g.start_time = time.time()

@app.after_request
def log_response(response):
    """Log response details."""
    duration = time.time() - g.start_time
    output = response.get_json()
    if len(output) > 10:
        logger.info(f"Response: {response.status_code} - Duration: {duration} -  Truncated: {response.get_json()}")
    else:
        logger.info(f"Response: {response.status_code} - Duration: {duration} -  {response.get_json()}")
    return response

# set up error handling
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': '404 Not Found'}), 404)

@app.errorhandler(500)
def internal_server_error(error):
    return make_response(jsonify({'error': 'Internal Server Error'}), 500)

# route 1: transactions per user
@app.route('/api/summary/transaction_totals', methods=['GET'])
def transaction_totals():
    try:
        connection = get_db_connection()
        result = connection.execute("SELECT * FROM UserTransactionTotals").fetchall()
        connection.close()
        summary = [
            {
                'user_id': row['user_id'], 
                'total_transaction_amount': row['total_transaction_amount']
            } 
            for row in result
        ]
        logger.info(f"Successfully fetched user transaction totals (transaction_totals)")
        return jsonify(summary)
    except Exception as e:
        logger.error(f"Database Error: {e}")
        return jsonify({"error": "An unexpected error occurred"})


# route 2: top 10 users by transaction volume
@app.route('/api/summary/top_users', methods=['GET'])
def top_users():
    try:
        connection = get_db_connection()
        result = connection.execute("SELECT * FROM TopUsers").fetchall()
        connection.close()
        summary = [
            {
                'user_id': row['user_id'],
                'total_transaction_amount': row['total_transaction_amount']
            } for row in result
        ]
        logger.info(f"Successfully fetched top users (top_users)")
        return jsonify(summary)
    except Exception as e:
        logger.error(f"Database Error: {e}")
        return jsonify({"error": "An unexpected error occurred"})

# route 3: daily transaction totals
@app.route('/api/summary/daily_transaction_totals', methods=['GET'])
def daily_transaction_totals():
    try:
        connection = get_db_connection()
        result = connection.execute("SELECT * FROM DailyTransactionTotals").fetchall()
        connection.close()
        summary = [
            {
                'transaction_date': row['transaction_date'], 
                'transaction_count': row['transaction_count'], 
                'transaction_amount': row['transaction_amount']
            } for row in result
        ]
        logger.info(f"Successfully fetched daily transaction totals (daily_transaction_totals)")
        return jsonify(summary)
    except Exception as e:
        logger.error(f"Database Error: {e}")
        return jsonify({"error": "An unexpected error occurred"})

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
