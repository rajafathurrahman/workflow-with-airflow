#Basic Airflow Library
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, date, timezone #library for time

default_args = {
    'owner': 'Hello',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1) }

dag = DAG('HelloWorld_01', default_args=default_args, schedule_interval=None)

def startProcess():
    print('FINISH')

def finishProcess():
    print('FINISH')

def printHelloworld(**kwargs):
    date = kwargs['ds']
    dates='{}'.format(date) 
    execute = datetime.strptime(dates, '%Y-%m-%d')
    date_now = (execute).strftime("%Y-%m-%d")
    
    print("Hello world!!, This is", date_now)

t1 = PythonOperator(
    task_id= 'startProcess',
    python_callable=startProcess,
    dag=dag)

t2 = PythonOperator(
    task_id= 'printHelloworld',
    provide_context=True,
    python_callable=printHelloworld,
    dag=dag)

t3 = PythonOperator(
    task_id= 'finishProcess',
    python_callable=finishProcess,
    dag=dag)

t1 >> t2 >> t3