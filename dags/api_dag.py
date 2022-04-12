import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from secret import API_KEY

def save_top_posts(ti) -> None:
    posts = ti.xcom_pull(task_ids=['get_top_posts'])
    with open('/Users/daviddugas/airflow/data/top.json', 'w') as f:
        json.dump(posts[0], f)

def save_pop_posts(ti) -> None:
    posts = ti.xcom_pull(task_ids=['get_pop_posts'])
    with open('/Users/daviddugas/airflow/data/pop.json', 'w') as f:
        json.dump(posts[0], f)


with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

 # 1. Check if the API is up
    task_is_api_top_active = HttpSensor(
        task_id='is_api_top_active',
        http_conn_id='api_posts',
        endpoint=f'topstories/v2/technology.json?api-key={API_KEY}'
    )

    task_is_api_pop_active = HttpSensor(
        task_id='is_api_pop_active',
        http_conn_id='api_posts',
        endpoint=f'mostpopular/v2/viewed/1.json?api-key={API_KEY}'
    )

    # 2. Get the posts
    task_get_top_posts = SimpleHttpOperator(
        task_id='get_top_posts',
        http_conn_id='api_posts',
        endpoint=f'topstories/v2/technology.json?api-key={API_KEY}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_get_pop_posts = SimpleHttpOperator(
        task_id='get_pop_posts',
        http_conn_id='api_posts',
        endpoint=f'mostpopular/v2/viewed/1.json?api-key={API_KEY}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

     # 3. Save the posts
    task_save_top = PythonOperator(
        task_id='save_top_posts',
        python_callable=save_top_posts
    )

    task_save_pop = PythonOperator(
        task_id='save_pop_posts',
        python_callable=save_pop_posts
    )

task_is_api_top_active >> task_is_api_pop_active >> task_get_top_posts >> task_get_pop_posts >> task_save_top >> task_save_pop