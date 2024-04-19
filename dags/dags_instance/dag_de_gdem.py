from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
 'owner': 'airflow',
 'start_date': datetime (2024, 4, 1)
}

# Definisi DAG dengan deskripsi
dag = DAG(
    'dag_de_gdem',
    default_args=default_args,
    schedule_interval='*/1 * * * *', #For testing purposes, the schedule_interval value is set to a period of 1 minute.
    catchup=False,
    description='This DAG is for GDEM (Global Digital Elevation Map) Data Extraction Purpose',
)

def initiateTask():
    import os
    import sys

    #Include currnt path for importing purpose
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    from source_code.GDEM.gdem import halo

    print ("=== Preparation for Retrieving GDEM Data ===")
    print ("current path: " , os.path.abspath(__file__))
    print ("on folder: ", os.path.dirname(os.path.abspath(__file__)))
    print ("sys path: ", sys.path)
    halo()
    return "Preparation"

def retrieveGDEMData():
    print ("=== Retrieving GDEM Data ===")
    return "Retrieveng"

# Definisi task
initiateTask = PythonOperator(
    task_id='initiate_task',
    python_callable=initiateTask,
    dag=dag

)

retrieveGDEMData = PythonOperator(
    task_id='retrieve_GDEM_data',
    python_callable=retrieveGDEMData,
    dag=dag)

# Mengatur urutan task dalam DAG
initiateTask >> retrieveGDEMData
