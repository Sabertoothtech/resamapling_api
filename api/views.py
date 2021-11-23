from django.shortcuts import render

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


@api_view(['GET'])
def sample_monthly(request,uid,pid):
    monthly_data =monthly[(monthly.user_id_id==float(uid)) & (monthly.portfolio_id_id == float(pid))]
    din = {}
    monthly_data['cr']=monthly_data['cr'].round(4)
    din = monthly_data
    din['mean'] = monthly_data['cr'].mean().round(4)
    din['std'] = monthly_data['cr'].std().round(4)
    din=din.fillna('')
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
    din['mean'] = weekly_data['cr'].mean().round(4)
    din['std'] = weekly_data['cr'].std().round(4)

    din = din.fillna('')
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
    din['mean']=quaterly_data['cr'].mean().round(4)
    din['std']=quaterly_data['cr'].std().round(4)
    din = din.fillna('')

    return Response(din)

