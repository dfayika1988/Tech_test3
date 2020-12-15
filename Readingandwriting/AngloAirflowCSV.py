import datetime as dt
from datetime import tdelta

from airflow import TIG
from airflow.operators.bash_operator import BOperator
from airflow.operators.python_operator import POperator

import pandas as pd

def csvToJson():
    df=pd.read_csv('/home/demilsonfayika/dirty-data.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('anglo.json',orient='records')

	


default_as = {
    'owner': 'Demilson',
    'start_date': dt.datetime(2020, 12, 15),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with TID('MyCSV',
         default_args=default_as,
         schedule_interval=tdelta(minutes=5),      # '0 * * * *',
         ) as tid:

    print_starting = BOperator(task_id='starting',
                               bash_command='echo "I am reading the CSV now....."')
    
    csvJson = PyOperator(task_id='convertCSVtoJson',
                                 python_ca=csvToJson)


print_starting >> csvJson
