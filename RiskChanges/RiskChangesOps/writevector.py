import psycopg2
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


def writeexposure(df, connstr, schema):
    engine = create_engine(connstr)
    # Execute the raw SQL statement to rename the column and add column value_exposure_rel, population_exposure_rel if not exists
    #! this is not required if updated existing table once
    with engine.connect() as connection:
        # try:
        #     connection.execute('''ALTER TABLE "{0}"."exposure_result" RENAME COLUMN "areaOrLen_exposure" TO "areaOrLen"'''.format(schema))
        # except:
        #     pass
        connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                    "exposure_result", 'value_exposure_rel', 'float', schema))
        connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                    "exposure_result", 'population_exposure_rel', 'float', schema))
    df.to_sql('exposure_result', engine, schema,
              if_exists='append', index=False)
    print('data written')

    engine.dispose()


def writeexposureAgg(df, connstr, schema):
    engine = create_engine(connstr)
    df.to_sql('exposure_result_agg', engine, schema,
              if_exists='append', index=False)
    print('data written')
    engine.dispose()


def writeLossAgg(df, connstr, schema):
    engine = create_engine(connstr)
    # print(engine)
    df.to_sql('loss_result_agg', engine, schema,
              if_exists='append', index=False)
    print('data written')

    engine.dispose()


def writeLoss(df, connstr, schema):
    engine = create_engine(connstr)

    df.to_sql('loss_result', engine, schema,
              if_exists='append', index=False)
    print('data written')

    engine.dispose()


def writeRisk(df, connstr, schema):
    engine = create_engine(connstr)
    df.to_sql('risk_result', engine, schema,
              if_exists='append', index=False)
    print('data written')
    engine.dispose()


def writeRiskAgg(df, connstr, schema):
    engine = create_engine(connstr)
    df.to_sql('risk_result_agg', engine, schema,
              if_exists='append', index=False)
    print('data written')

    engine.dispose()


def writeRiskCombined(df, connstr, schema):
    engine = create_engine(connstr)
    df.to_sql('risk_result_combined', engine, schema,
              if_exists='append', index=False)
    print('data written')

    engine.dispose()
