from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, date, timezone #library for time
import pandas as pd ## Library untuk penggunaan Dataframe  
import pymysql ## Koneksi ke mysql

from airflow.hooks.base_hook import BaseHook ## Menyimpan User password database di Airflow

default_args = {
    'owner': 'hook_connection',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 29)
}

dag = DAG('Hook_Example', default_args=default_args, schedule_interval=None)

def makeConnection(host, database, user='', password='',port=3306):
    return pymysql.connect(host=host,port=int(port),user=user,passwd=password,db=database,charset='utf8mb4')

ConflocalDB = BaseHook.get_connection("localDB") ## Connect to database local
connWarehouseWhmcs = makeConnection(ConflocalDB.host, ConflocalDB.schema, ConflocalDB.login, ConflocalDB.password, ConflocalDB.port)

def fetchDataFromTable(query,conn):
    results = pd.read_sql_query(query, conn)
    return results

def getdata(conn):
    query = """
            select *
            from monitoring
            """
    getData = fetchDataFromTable(query,conn)
    return getData

def startProcess():
    print('FINISH')

def finishProcess():
    print('FINISH')

def getData():
    
    data_monitoring = getdata(connWarehouseWhmcs)

    return data_monitoring

t1 = PythonOperator(
    task_id= 'startProcess',
    python_callable=startProcess,
    dag=dag)

t2 = PythonOperator(
    task_id= 'getData',
    python_callable=getData,
    dag=dag)

t3 = PythonOperator(
    task_id= 'finishProcess',
    python_callable=finishProcess,
    dag=dag)

t1 >> t2 >> t3
