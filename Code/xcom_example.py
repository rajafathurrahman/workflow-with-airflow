from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2)}

dag = DAG('example_xcom',default_args=args, schedule_interval="@once")

value_2 = {'a': 'b'}

def push(**kwargs):
    ti = kwargs['task_instance']
    value = [1, 2, 3]
    ti.xcom_push(key='value_1', value=value)

def push_by_returning(**kwargs):
    return value_2

def puller(**kwargs):
    ti = kwargs['ti']

    pulled_value_1 = ti.xcom_pull(key='value_1', task_ids='push')
    print('Xcom get value_1 : ',pulled_value_1)
    pulled_value_2 = ti.xcom_pull(task_ids='push_by_returning')
    print('Xcom get values_2',pulled_value_2)

push1 = PythonOperator(
    task_id='push',
    python_callable=push,
    provide_context=True,
    dag=dag)

push2 = PythonOperator(
    task_id='push_by_returning',
    python_callable=push_by_returning,
    dag=dag)

pull = PythonOperator(
    task_id='puller',
    python_callable=puller,
    provide_context=True,
    dag=dag)

[push1, push2] >> pull