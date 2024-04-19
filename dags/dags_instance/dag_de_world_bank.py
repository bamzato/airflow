from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
 'owner': 'airflow',
 'start_date': datetime (2024, 4, 1)
}

# Definisi DAG dengan deskripsi
dag = DAG(
    'dag_de_world_bank',
    default_args=default_args,
    schedule_interval='*/1 * * * *', #For testing purposes, the schedule_interval value is set to a period of 1 minute.
    catchup=False,
    description='This DAG is for World Bank Data Extraction Purpose',
)

# Definisi task
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Mengatur urutan task dalam DAG
start_task >> end_task
