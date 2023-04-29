from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def cumprimentos():
    print("Boas-vindas ao Airflow!")


with DAG(
    'train',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')
    task4 = PythonOperator(
        task_id='func',
        python_callable=cumprimentos,
        op_kwargs={},
        dag=dag)
    task1 >> [task2, task3]
    task3 >> task4
