from django.shortcuts import render
from math import sqrt
# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sqlalchemy import create_engine,text
import pandas as pd
from pandas.tseries.frequencies import to_offset
import os

password = "jw8s0F4"
engine = create_engine('postgresql://manuel:{}@50.116.32.224:5432/pradeep_test'.format(password))
sql = engine.connect()

df=pd.read_sql_table("dashboard_api_portfolioperformance",con=engine)
df['portfolio_id_id']=df['portfolio_id_id'].fillna('0.2')
df.drop_duplicates(inplace=True)

sam=df.groupby('user_id_id')
global sam1
sam1=df.groupby('user_id_id')

# monthly = pd.read_sql_table("calculated_resampleMonthly", con=engine)
#
# weekly = pd.read_sql_table("calculated_resampleweekly", con=engine)
#
#
# quaterly = pd.read_sql_table("calculated_resamplequaterly", con=engine)
#
# daily_return=pd.read_sql_table("daily_return",con=engine)
#

def resample(days):
    for i in range(1, len(sam1.groups)):

        name2 = sam1.get_group(i)
        sample2 = name2.groupby('portfolio_id_id')
        for j in sample2.groups:
            kam = (sample2.get_group(j))
            kam = kam.set_index(pd.DatetimeIndex(kam['date']))
            kam = kam.drop(columns=['date'])
            kam = kam.sort_values(by='date')
            kam.index.name = 'date'

            logic = {'total_value': 'first',
                     'user_id_id': 'max',
                     'portfolio_id_id': 'max',
                     }
            nam = kam.resample(str(days) + 'd', convention='end').apply(logic)
            kam.index -= to_offset("6D")

            # nam['cr']=(1+nam['total_value']).cumprod()-1
            # nam['user_id']=kam['user_id_id']
            # nam['portfolio_id']=kam['portfolio_id_id']
            nam['cr'] = 0

            # print(kam)
            # nam['cr']=0
            df1 = {}
            nam_new = pd.DataFrame()
            for k in range(len(nam)):
                try:
                    etr = (nam['total_value'][k] - nam['total_value'][k + 1]) / nam['total_value'][k]
                    # nam.loc['cr'][k]=9
                    df1 = nam.iloc[k]
                    df1['cr'] = etr
                    nam_new = nam_new.append([df1], ignore_index=False)
                    nam_new = nam_new.drop_duplicates()


                except:
                    pass
                nam_new.drop_duplicates(inplace=True)
                # print(nam_new)
                if not os.path.isfile(days + '.csv'):
                    # nam_new=nam_new.set_index(pd.DatetimeIndex(nam_new['date']))
                    nam_new.index.name = 'date'
                    nam_new = nam_new.sort_values(by='date')
                    nam_new = nam_new.drop_duplicates()
                    nam_new.to_csv(days + '.csv', header='column_names', index='date')
                else:  # else it exists so append without writing the header
                    nam_new.to_csv(days + '.csv', mode='a', header=False, index='date')
    #     #                     nam.to_csv('calculated_resampleweekly.csv',
    #                             #con=engine,
    #                             if_exists='append')
    sample=nam_new.columns
    print(sample)
    act = pd.read_csv(days + '.csv')


    act = act.set_index(pd.DatetimeIndex(act['date']))
    act = act.drop(columns=['date'])
    act = act.sort_values(by='date')
    act.index.name = 'date'
    # act=act.set_index(pd.DatetimeIndex(act['date']))
    act.drop_duplicates(inplace=True)
    act.fillna(0, inplace=True)
    os.remove(days + '.csv')
    print(len(act))
    if len(act)==2:
        act= act.reset_index(inplace=True)
        print(act.columns)
        act.columns = ['date','total_value','user_id_id','portfolio_id_id','cr']
        return act
    #act.to_csv(days + '.csv', index='date')

    return act

    # print(etr)
    # print(df)
    # print(nam)


@api_view(['GET'])
def sample_monthly(request,uid,pid,days):
    monthly = resample(days)
    if len(monthly)==0:
        data=pd.DataFrame(columns=df.columns)
        #data.insert(0,'na','na','na')
        #data = data.columns.ffill('NA')
        return Response(data)

    monthly_data =monthly[(monthly.user_id_id==float(uid)) & (monthly.portfolio_id_id == float(pid))]
    din = {}
    monthly_data['cr']=monthly_data['cr'].round(4)
    din = monthly_data
    din['Date']=din.index
    din['mean'] = round((monthly_data['cr'].mean()*12),4)
    din['std'] = round((monthly_data['cr'].std()*sqrt(12)),4)
    din['sharpe_ratio'] = round((din['mean'] / din['std']),4)
    din=din.fillna('NA')
    print(din.fillna(''))
    return Response(din)


@api_view(['GET'])
def sample_weekly(request,uid,pid,days):
    weekly = resample(days)
    if len(weekly)==0:
        data=pd.DataFrame(columns=df.columns)
        #data.insert(0,'na','na','na')
        #data = data.columns.ffill('NA')
        return Response(data)

    weekly_data =weekly[(weekly.user_id_id==float(uid)) & (weekly.portfolio_id_id == float(pid))]
    #& (monthly.portfolio_id_id == pid)
    #print(monthly)
    din = {}
    weekly_data['cr']=weekly_data['cr'].round(4)
    din = weekly_data.drop_duplicates()
    din['mean'] = (weekly_data['cr'].mean()*52).round(4)
    din['std'] = (weekly_data['cr'].std()*sqrt(52)).round(4)
    din['sharpe_ratio'] = (din['mean'] / din['std']).round(4)
    din = din.fillna('NA')
    return Response(din)


@api_view(['GET'])
def sample_quaterly(request,uid,pid,days):
    quaterly=resample(days)
    if len(quaterly)==0:
        data=pd.DataFrame(columns=df.columns)
        #data.insert(0,'na','na','na')
        #data = data.columns.ffill('NA')
        return Response(data)



    quaterly_data =quaterly[(quaterly.user_id_id==float(uid)) & (quaterly.portfolio_id_id == float(pid))]
    #& (monthly.portfolio_id_id == pid)
    #print(monthly)
    # print(quaterly_data['cr'].mean())
    # mean_qaut=quaterly_data
    # print(type(mean_qaut))
    din={}
    quaterly_data['cr']=quaterly_data['cr'].round(4)
    din=quaterly_data
    din['mean']=(quaterly_data['cr'].mean()*4).round(4)
    din['std']=(quaterly_data['cr'].std()*sqrt(4)).round(4)
    din['sharpe_ratio'] = (din['mean'] / din['std']).round(4)
    din = din.fillna('NA')

    return Response(din)

@api_view(['GET'])
def daily(request,uid,pid,days):
    daily_return=resample(days)
    if len(daily_return)==0:
        data=pd.DataFrame(columns=df.columns)
        #data.insert(0,'na','na','na')
        #data = data.columns.ffill('NA')
        return Response(data)



    dri = daily_return[(daily_return['portfolio_id_id'] == float(pid)) & (daily_return['user_id_id'] == float(uid))]
    dri['rm'] = 0

    # simple moving average
    def SMA(data, period=30, column='dr'):
        return data[column].rolling(window=period).mean()

    if len(dri) > 30:
        dri = dri.drop(columns=['level_0'])
        dri.reset_index(inplace=True)
        dri['rm'] = SMA(dri, 30)
        for i in range(0, 30):
            dri['rm'][i] = dri['dr'][0:i].mean()
    else:
        dri = dri.drop(columns=['level_0'])
        dri.reset_index(inplace=True)
        for i in range(len(dri)):
            dri['rm'][i] = dri['dr'][0:i].mean()

    dri = dri.fillna(0.0)
    dri['dr']=dri['dr'].round(4)
    dri['rm']=(dri['rm']*365).round(4)



    return Response(dri)






