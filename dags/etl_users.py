
import json
import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


default_args = {
    'start_date': datetime(year=2021, month=5, day=12)
}

def extract_users(url: str, ti) -> None:
    res = requests.get(url)
    json_data = json.loads(res.content)
    ti.xcom_push(key='extracted_users', value=json_data)

def transform_users(ti) -> None:
    users = ti.xcom_pull(key='extracted_users', task_ids=['extract_users'])[0]
    transformed_users = []
    for user in users:
        transformed_users.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })
    ti.xcom_push(key='transformed_users', value=transformed_users)

def load_users(path: str, ti) -> None:
    users = ti.xcom_pull(key='transformed_users', task_ids=['transform_users'])
    users_df = pd.DataFrame(users[0])
    users_df.to_csv(path, index=None)


with DAG(
    dag_id='etl_users',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing users'
) as dag:

    # Task 1 - Fetch user data from the API
    task_extract_users = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
        op_kwargs={'url': 'https://jsonplaceholder.typicode.com/users'}
    )

    # Task 2 - Transform fetched users
    task_transform_users = PythonOperator(
        task_id='transform_users',
        python_callable=transform_users
    )

    # Task 3 - Save users to CSV
    task_load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_users,
        op_kwargs={'path': '/home/airflow/airflow/data/users.csv'}
    )

    task_extract_users >> task_transform_users >> task_load_users