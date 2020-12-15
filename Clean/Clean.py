import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd



def datadirty():
    df=pd.read_csv('data-dirty.csv')
    df.drop(columns=['guid'], inplace=True)
    df.columns=[x.lower() for x in df.columns]
    df['started_date']=pd.to_datetime(df['started_date'],format='%m/%d/%Y %H:%M')
    df.to_csv('data-clean.csv')


def filterData():
    df=pd.read_csv('data-clean.csv')
    tofrom = df[(df['birthday']>fromd)&(df['birthday']<tod)]
    tofrom.to_csv('anglodata.csv')	


default_args = {
    'owner': 'Demilson',
    'start_date': dt.datetime(2020, 12, 15),
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('CData',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      
         ) as dag:

    cleanData = PythonOperator(task_id='clean',
                                 python_callable=cleanScooter)
    
    selectData = PythonOperator(task_id='filter',
                                 python_callable=filterData)

    moveFile = BashOperator(task_id='move',
                                 bash_command='mv /home/demilsonfayika/anglodata.csv /home/paulcrickard/Desktop')



cleanData >> selectData >> moveFile
