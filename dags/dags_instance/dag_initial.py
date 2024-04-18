from airflow import DAG
from datetime import datetime

# Operator
from airflow.operators.python_operator import PythonOperator

#Define default arguments
default_args = {
 'owner': 'airflow',
 'start_date': datetime (2023, 4, 5)
}

# Instantiate your DAG
# https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
period = '*/1 * * * *'
# once = '@once'
# daily = '@daily'
dag = DAG ('my_first_dag', default_args=default_args, schedule_interval=period)

# Define tasks
def task1():
    print ("Melakukan task 1")
    return "Task 1"

def task2():
    print ("Executing Task 2")
    return "Task 2"

def task3():
    print ("Executing Task 3")
    return "Task 3"


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1,
    dag=dag
)
task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2,
    dag=dag
)
task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task3,
    dag=dag
)

# Set task dependencies
[task_2, task_3] >> task_1