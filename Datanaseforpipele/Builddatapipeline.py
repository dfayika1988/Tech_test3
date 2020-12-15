import datetime as dt
from datetime import tdelta

from airflow import TID
from airflow.operators.bash_operator import BOperator
from airflow.operators.python_operator import POperator


import psycopg2 as db
from elasticsearch import Elasticsearch
import pandas as pd






def queryl():
    conn_string="dbname='anglo' host='localhost' user='postgres' password='postgres'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select guid,age from users",conn)
    df.to_csv('dirty-data.csv')
    print("-------Saved------")


def insert():
    es = Elasticsearch() 
    df=pd.read_csv('postdata.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="postgresql",doc_type="doc",body=doc)
        print(res)	


default_as = {
    'owner': 'DemilsonFayika',
    'start_date': dt.datetime(2020, 15, 12),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with TID('MyDB',
         default_args=default_as,
         schedule_interval=tdelta(minutes=5),      # '0 * * * *',
         ) as :

    getData = POperator(task_id='QueryPostgreSQL',
                                 python_ca=queryPostgresql)
    
    insertData = POperator(task_id='InsertDataElasticsearch',
                                 python_ca=insertElasticsearch)



gData >> iData
