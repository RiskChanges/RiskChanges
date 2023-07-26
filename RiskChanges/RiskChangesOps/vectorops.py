from rasterio.windows import Window
from rasterio.windows import from_bounds
from rasterio.mask import mask
import rasterio
import psycopg2
# import geopandas as gpd
import pandas as pd

def cehckprojection(ear,haz):
    ear_epsg=ear.crs.to_epsg()
    haz_epsg=haz.crs.to_epsg()
    if ear_epsg==haz_epsg:
        return False
    else:
        return True

def changeprojection(ear,haz):
    #ear_epsg=ear.crs.to_epsg()
    haz_epsg=haz.crs.to_epsg()
    ear=ear.to_crs(epsg=haz_epsg)
    return ear

def readSQL(connstr, sql):
    engine = psycopg2.connect(connstr)
    try:
        # table_data = gpd.read_postgis(sql, con=engine)
        table_data=pd.read_sql(sql,engine)
        engine.close()
        return True, table_data
    except Exception as e:
        engine.close()
        return False, str(e)


    
