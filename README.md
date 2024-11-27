# Transactions App

## Modules

### Data Ingestion Module (`data_ingestion.py`)

* Loads cleaned CSV files into a SQLite database
* Checks data quality (missing values, duplicates, data types) before loading it into the database

### ETL Module (`etl.py`)

* Extracts data from the SQLite database and applies aggregations
* Saves the transformed data back into the database in new tables:
    - `UserTransactionTotals`: total transaction amount per user
    - `TopUsers`: top 10 users by transaction volume
    - `DailyTransactionTotals`: daily transaction totals across all users

### API Module (`app.py`)

* Sets up routes to query precomputed aggregated data listed above

## Monitoring and Logging

Logging is performed in each module. Logs can be found in the `app.log` file. 

### Events to log:
* requests
* responses
* errors (+ types; ie. database, network, etc.)

### Monitor application health

Monitor the following values:
* error rates
* response time
* query speed
* resource utilization (memory, CPU, etc.)

These metrics can be monitored in real-time using a tool such as Datadog, and alerts should be set up for performance degredation.

## Testing

Basic unit tests, integration tests, and database tests are included to validate data format and function usage is included in the `tests` module for for data ingestion and Flask API modules. These include:
* test individual functions
* verify error handling mechanisms return expected error types and messages
* check for proper status codes and response fields from api routes

Next steps time permitting:
* test that error handling throws the correct error types and returns expected messages
* test additional edge cases for functinons
* load tests (simulate high volume requests, monitor response times and erorr rates)
* security tests (ex. prevent SQL injection)

Additionally, no tests were added for `etl.py` due to time constaints.

## Containerization

A Dockerfile is included which installs required packages and runs the data ingestion and ETL scripts, and exposes the port 5000 and runs app.py. This has not been thoroughtly tested due to time constraints. 
