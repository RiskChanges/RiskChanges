#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#load shapefile in the SDSS system


# In[4]:



import geopandas
import numpy
import os
import sys
from rasterstats import zonal_stats
from shapely.geometry import LineString
from shapely import wkt
from owslib.wcs import WebCoverageService 


# In[8]:


# Imports
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd


# In[69]:


def loadshp(shpInput,connstr,lyrName,schema='public', index=None):  
    #Load data in geodataframe
    geodataframe = geopandas.read_file(shpInput) 
    
    #Identify CRS
    crs_name=str(geodataframe.crs.srs)
    #crs_name=str(geodataframe.crs['init'])
    ##type(crs_name)
    epsg=int(crs_name.replace('epsg:',''))
    if epsg is None:
        epsg=4326
    #Identify Geometry type
    geom_type=geodataframe.geom_type[0]
    #print(geom_type)
    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)

    geodataframe['index'] = index
    
    #... [do something with the geodataframe]

    geodataframe['geom'] = geodataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=epsg))

    #drop the geometry column as it is now duplicative
    geodataframe.drop('geometry', 1, inplace=True)

    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'
    geodataframe.to_sql(lyrName, engine, schema,if_exists='append', index=False, 
                             dtype={'geom': Geometry(geom_type, srid= epsg)})


# In[70]:


#loadshp(shpInput="D:\SDSS\Sample data\Split_one\Flood_Class.shp",
#        connstr='postgresql://postgres:puntu@localhost:5432/postgres',lyrName='Tekson',schema='tekson')

