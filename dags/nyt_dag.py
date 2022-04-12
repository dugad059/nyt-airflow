import json
import time
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from secret import API_KEY


def get(url: str) -> None:
    endpoint = url.split('/')[4]
    now = datetime.now()
    now = f"{now.year}-{now.month}-{now.day}T{now.hour}-{now.minute}-{now.second}"
    res = requests.get(url)
    res = json.loads(res.text)

    with open(f"/Users/daviddugas/airflow/data/{endpoint}-{now}.json", 'w') as f:
        json.dump(res, f)
    time.sleep(2)


with DAG(
    dag_id='nyt_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    task_get_top_stories = PythonOperator(
        task_id='get_top',
        python_callable=get,
        op_kwargs={'url': f'https://api.nytimes.com/svc/topstories/v2/technology.json?api-key={API_KEY}'}
    )

    task_get_popular_articles = PythonOperator(
        task_id='get_popular',
        python_callable=get,
        op_kwargs={'url': f'https://api.nytimes.com/svc/mostpopular/v2/viewed/1.json?api-key={API_KEY}'}
    )



  

task_start >> [task_get_top_stories, task_get_popular_articles]




