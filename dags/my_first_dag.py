from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

with DAG(
    'my_first_dag',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')
    task4 = BashOperator(
        task_id='mkdir',
        bash_command='mkdir -p "/home/nicholas10/airflow/test_dir"')
    task1 >> [task2, task3]
    task3 >> task4
