# Automating Data Collection and Storage
Develop an automated ETL (Extract, Transform, Load) pipeline that connects to an external API to retrieve data, processes the data as per requirements, and stores it in a PostgreSQL database. The process is implemented using Apache Airflow.

# Technology Stack
- ***Apache Airflow***: Workflow orchestration and DAG management.
- ***PostgreSQL***: For storing the processed data.
- ***Python***: For implementing the ETL process.
- ***Docker***: For running the project in an isolated environment.

# Key Features
### 1. Automated Data Loading
- The DAG (Directed Acyclic Graph) orchestrates the execution of tasks scheduled at regular intervals (e.g., every minute, hourly, or on demand).
### 2. API Integration
- A Python script connects to a cryptocurrency API service (CoinMarketCap).
- Retrieves key information such as cryptocurrency name, current price, and timestamp.
### 3. Data Processing
- The retrieved data undergoes transformations (e.g., filtering, time formatting, restructuring).
### 4. Data Storage in PostgreSQL
- The PostgresHook is used to import processed data into the database.
- Creates a table with the appropriate schema if it doesn't already exist.
