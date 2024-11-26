Workflow

Data Ingestion: Raw CSV files → Cleaned tables in SQLite (users, transactions).
ETL Process: Raw tables → Transformed tables in SQLite (UserTransactionTotals, TopUsers, DailyTotals).
API Layer: Transformed tables → Exposed via Flask RESTful APIs.