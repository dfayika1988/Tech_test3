import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BOperator
from airflow.operators.python_operator import POperator

import pandas as pd



def datadirty():
    df=pd.read_csv('data-dirty.csv')
    df.drop(columns=['guid'], inplace=True
    df['signup_date']=pd.to_datetime(df['signup_date'],format='%m/%d/%Y %H:%M')
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

    clean = POperator(task_id='clean',
                                 python_ca=clean-data)
    
    sData = POperator(task_id='filter',
                                 python_ca=filterData)

    moveFile = Bperator(task_id='move',
                                 bash_command='mv /home/demilsonfayika/clean-data.csv /home/demilsonfayika/Desktop')



clean >> sData >> moveFile
