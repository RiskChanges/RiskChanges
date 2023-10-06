import psycopg2
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


def writeexposure(df, connstr, schema,table_name="exposure_result"):
    print("write_vector called")
    engine = create_engine(connstr)
    # Execute the raw SQL statement to rename the column and add column value_exposure_rel, population_exposure_rel if not exists
    #! this is not required if updated existing table once
    with engine.connect() as connection:
        # try:
        #     connection.execute('''ALTER TABLE "{0}"."exposure_result" RENAME COLUMN "areaOrLen_exposure" TO "areaOrLen"'''.format(schema))
        # except:
        #     pass
        if table_name=="exposure_result":
            try:
                connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                            table_name, 'value_exposure_rel', 'float', schema))
                connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                            table_name, 'population_exposure_rel', 'float', schema))
                connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                        table_name, 'count', 'float', schema))
                connection.execute('''ALTER TABLE "{3}"."{0}" ADD IF NOT EXISTS "{1}" {2}'''.format(
                        table_name, 'count_rel', 'float', schema))
            except Exception as e:
                #if exposure_result table does not exists
                print(str(e))
                
        try:
            df.to_sql(table_name, engine, schema,index=False)
            if table_name=="exposure_result":
                connection.execute('''CREATE INDEX {0} ON {1}.{2} ({3})'''.format("idx_exp_id",schema,table_name,"exposure_id"))
                connection.execute('''CREATE INDEX {0} ON {1}.{2} ({3})'''.format("idx_exp_admin_id",schema,table_name,"admin_id"))
            elif table_name=="raster_exposure_result":
                connection.execute('''CREATE INDEX {0} ON {1}.{2} ({3})'''.format("idx_ras_exp_id",schema,table_name,"exposure_id"))
                connection.execute('''CREATE INDEX {0} ON {1}.{2} ({3})'''.format("idx_ras_exp_admin_id",schema,table_name,"admin_id"))
            # CREATE INDEX "idx_exp_id" ON "test_rabina_organization"."exposure_result" ("exposure_id")
            # CREATE INDEX "idx_exp_admin_id" ON "test_rabina_organization"."exposure_result" ("admin_id")
            # CREATE INDEX "idx_ras_exp_id" ON "test_rabina_organization"."raster_exposure_result" ("exposure_id")
            # CREATE INDEX "idx_ras_exp_admin_id" ON "test_rabina_organization"."raster_exposure_result" ("admin_id")
        except Exception as e:
            print(str(e),"create index exceptoionnnnnnnnnnnnnnnn")
            df.to_sql(table_name, engine, schema,
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
