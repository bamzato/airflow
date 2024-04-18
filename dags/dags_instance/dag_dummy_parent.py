from airflow import DAG
from datetime import datetime

# Operator
from airflow.operators.python_operator import PythonOperator

#Define default arguments
default_args = {
 'owner': 'airflow',
 'start_date': datetime (2024, 4, 18)
}

# Instantiate your DAG
# https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
period = '*/1 * * * *'
# once = '@once'
# daily = '@daily'
dag = DAG ('dag_dummy_parent', default_args=default_args, schedule_interval=period)

# Define tasks
def leave_work():
    print ("Ini adalah task leave work dari dag parent")
    return "Leave Work"

def cook_dinner():
    print ("Ini adalah task cook dinner dari dag parent")
    print(b)
    return "Cook Dinner"

leave_work = PythonOperator(
    task_id='leave_work',
    python_callable=leave_work,
    dag=dag
)

cook_dinner = PythonOperator(
    task_id='cook_dinner',
    python_callable=cook_dinner,
    dag=dag
)

# Set task dependencies
leave_work >> cook_dinner