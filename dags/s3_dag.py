from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )
	

     # Upload the file
    task_upload_top_to_s3 = PythonOperator(
        task_id='upload_top_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/daviddugas/airflow/data/top.json',
            'key': 'top.json',
            'bucket_name': 'nyt-airflow'
        }
    )

    task_upload_pop_to_s3 = PythonOperator(
        task_id='upload_pop_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/daviddugas/airflow/data/pop.json',
            'key': 'pop.json',
            'bucket_name': 'nyt-airflow'
        }
    )


task_start >>[task_upload_top_to_s3, task_upload_pop_to_s3]