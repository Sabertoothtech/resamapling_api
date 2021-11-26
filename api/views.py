from django.shortcuts import render
from math import sqrt
# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
import pandas as pd
from sqlalchemy import create_engine

password = "jw8s0F4"
engine = create_engine('postgresql://manuel:{}@50.116.32.224:5432/pradeep_test'.format(password))
sql = engine.connect()
monthly = pd.read_sql_table("calculated_resampleMonthly", con=engine)

weekly = pd.read_sql_table("calculated_resampleweekly", con=engine)


quaterly = pd.read_sql_table("calculated_resamplequaterly", con=engine)

daily_return=pd.read_sql_table("daily_return",con=engine)


@api_view(['GET'])
def sample_monthly(request,uid,pid):
    monthly_data =monthly[(monthly.user_id_id==float(uid)) & (monthly.portfolio_id_id == float(pid))]
    din = {}
    monthly_data['cr']=monthly_data['cr'].round(4)
    din = monthly_data
    din['mean'] = (monthly_data['cr'].mean()*12).round(4)
    din['std'] = (monthly_data['cr'].std()*sqrt(12)).round(4)
    din['sharpe_ratio'] = (din['mean'] / din['std']).round(4)
    din=din.fillna('NA')
    print(din.fillna(''))
    return Response(din)


@api_view(['GET'])
def sample_weekly(request,uid,pid):
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
def sample_quaterly(request,uid,pid):
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
def daily(request,uid,pid):
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






