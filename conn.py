import pandas as pd
import numpy as np
from sqlalchemy import create_engine,text
import pandas as pd
from pandas.tseries.frequencies import to_offset
import os

import time

def connect():

    password = "jw8s0F4"
    global engine
    li=os.listdir()
    if 'dashboard_api_portfolioperformance.csv' in li:
        os.remove('dashboard_api_portfolioperformance.csv')
    engine = create_engine('postgresql://manuel:{}@50.116.32.224:5432/pradeep_test'.format(password))
    sql = engine.connect()

    df=pd.read_sql_table("dashboard_api_portfolioperformance",con=engine)
    df.to_csv('dashboard_api_portfolioperformance.csv')
    return print("connected at ")


while True:
    connect()
    time.sleep(60)