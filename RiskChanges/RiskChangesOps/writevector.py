import psycopg2
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine

def writeexposure(df,connstr,schema):  
    engine = create_engine(connstr)
    print(engine)
    try:
        df.to_sql('exposure_result', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Exposure") #auto add its primary key
    engine.dispose()

def writeexposureAgg(df,connstr,schema):  
    engine = create_engine(connstr)
    print(engine)
    try:
        df.to_sql('exposure_result_agg', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Exposure") #auto add its primary key
    engine.dispose()

    
def writeLossAgg(df,connstr,schema):
    engine = create_engine(connstr)
    #print(engine)
    try:
        df.to_sql('loss_result_agg', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Loss") #auto add its primary key
    engine.dispose()

def writeLoss(df,connstr,schema):
    engine = create_engine(connstr)
    #print(engine)
    try:
        df.to_sql('loss_result', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Loss") #auto add its primary key
    engine.dispose()

def writeRisk(df,connstr,schema):
    engine = create_engine(connstr)
    #print(engine)
    try:
        df.to_sql('risk_result', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Risk") #auto add its primary key
    engine.dispose()
def writeRiskAgg(df,connstr,schema):
    engine = create_engine(connstr)
    #print(engine)
    try:
        df.to_sql('risk_result_agg', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Risk") #auto add its primary key
    engine.dispose()


def writeRiskCombined(df,connstr,schema):
    engine = create_engine(connstr)
    #print(engine)
    try:
        df.to_sql('risk_result_combined', engine, schema,if_exists='append', index=False)
        print('data written')

    except:
        print("error, trying to append in non related table,please store in same table as Risk") #auto add its primary key
    engine.dispose()