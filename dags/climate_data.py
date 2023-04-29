#OS
from os.path import join
#DATA
import pandas as pd
import pendulum
#AIRFLOW 
from airflow.macros import ds_add
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'climate_dag',
    start_date=pendulum.datetime(2023, 3, 25, tz="UTC"),
    schedule_interval='0 0 * * 1',  #EXECUTE EVERY MONDAY
) as dag:

    task1 = BashOperator(
        task_id='mkdir',
        bash_command='mkdir -p "/home/nicholas10/airflow/week={{data_interval_end.strftime("%Y-%m-%d")}}"')

    def extract_data(data_interval_end):
        city = 'Boston'
        key = '4CMGPBH2ECUKV3BY8CJURNLAY'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                   f'{city}/{data_interval_end}/{ds_add(data_interval_end,7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        data = pd.read_csv(URL)
        file_path = f'/home/nicholas10/airflow/week={data_interval_end}/'
        data.to_csv(file_path + 'raw_data.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(
            file_path + 'temperatures.csv')
        data[['datetime', 'description', 'icon']].to_csv(
            file_path + 'climate_conditions.csv')

    task2 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
            'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}

    )
    task1 >> task2
