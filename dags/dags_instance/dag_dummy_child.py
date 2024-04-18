from airflow import DAG
from datetime import datetime

# Operator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

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
dag = DAG ('dag_dummy_child', default_args=default_args, schedule_interval=period)

# Define tasks
def have_dinner():
    print ("Ini adalah task have dinner dari dag child")
    return "Leave Work"

def play_with_food():
    print ("Ini adalah task play with food dari dag child")
    return "Cook Dinner"

wait_for_dinner = ExternalTaskSensor(
    task_id='wait_for_dinner',
    external_dag_id='dag_dummy_parent',
    external_task_id='cook_dinner',
    start_date=datetime(2024, 4, 18),
    timeout=5,
    mode='poke'
)

have_dinner = PythonOperator(
    task_id='have_dinner',
    python_callable=have_dinner,
    dag=dag
)

play_with_food = PythonOperator(
    task_id='play_with_food',
    python_callable=play_with_food,
    dag=dag
)

# Set task dependencies
wait_for_dinner >> [have_dinner, play_with_food]