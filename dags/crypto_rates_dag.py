from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum


def extract(**kwargs):
    from config import api_url, api_key   # import info about API-key from file with configuration
    from requests import Session
    
    import json

    # creating 'session' and update header
    url = api_url
    parameters = {
        'start':'1',
        'limit':'10',
        'convert':'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }

    session = Session()
    session.headers.update(headers)

    try:
        # get data from API
        response = session.get(url, params=parameters)
        data = json.loads(response.text)

        return data 
    except Exception as ex:
        print('[INFO] Error in extract method:', ex)
        

def transform(ti, **kwargs):
        
        data_from_api = ti.xcom_pull(task_ids='extract_task')
        try :
            crypto_rates = {}

            crypto_rates['name'] = data_from_api['data'][0]['name']
            crypto_rates['price'] = data_from_api['data'][0]['quote']['USD']['price']
            crypto_rates['time'] = data_from_api['status']['timestamp']

            return crypto_rates
        except Exception as ex:
            print('[INFO] Error in transform method:', ex)
        

def load(ti, **kwargs):
    
    crypto_data = ti.xcom_pull(task_ids='transform_task')

    try:
        hook = PostgresHook(postgres_conn_id='crypto_rates')
        
        # initialize table 'crypto_rates'
        hook.run(sql_schema_init)

        # add data to PostgreSQL
        hook.run("INSERT INTO crypto_rates (cryptocurrency, time, price) VALUES (%s, %s, %s)",
             parameters=(crypto_data['name'], crypto_data['time'], crypto_data['price']))

        sql_schema_init = """
            CREATE TABLE IF NOT EXISTS crypto_rates (
            id SERIAL PRIMARY KEY,
            cryptocurrency VARCHAR(100),
            time TIMESTAMP,
            price FLOAT(10)
        );
        """

        print('[INFO] Data inserted successfully')
    except Exception as ex:
        print('[INFO] Error in load:', ex)


with DAG(
    dag_id='crypto_rates_dag',
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2024, 12, 10, tz="UTC"),
    catchup=False,
    tags=["crypto_rates"]
) as dag:    
    
    python_extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    python_transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    python_load_task = PythonOperator(
        task_id='load_task',
        python_callable=load 
    )
                
    python_extract_task >> python_transform_task >> python_load_task
