#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Load WCS Module


# In[4]:


import geopandas as gpd
from requests import Request
from owslib.wfs import WebFeatureService
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd


# In[15]:


def LoadWFS(wfsURL,layer_name,connstr,lyrName,schema):
    # URL for WFS backend
    url = wfsURL

    # Initialize
    wfs = WebFeatureService(url=url)
    # Get data from WFS
    # ----------------
    # Fetch the last available layer (as an example) --> 'vaestoruutu:vaki2017_5km'
    layer = layer_name
    # Specify the parameters for fetching the data
    params = dict(service='WFS', version="1.0.0", request='GetFeature',
          typeName=layer, outputFormat='json')
    # Parse the URL with parameters
    q = Request('GET', url, params=params).prepare().url
    # Read data from URL
    geodataframe = gpd.read_file(q)
    
    
    #Identify CRS
    crs_name=str(geodataframe.crs['init'])
    ##type(crs_name)
    epsg=int(crs_name.replace('epsg:',''))
    if epsg is None:
        epsg=4326
    #Identify Geometry type
    geom_type=geodataframe.geom_type[0]
    #print(geom_type)
    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)
    
    #... [do something with the geodataframe]

    geodataframe['geom'] = geodataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=epsg))

    #drop the geometry column as it is now duplicative
    geodataframe.drop('geometry', 1, inplace=True)

    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'
    geodataframe.to_sql(lyrName, engine, schema='tekson',if_exists='append', index=False, 
                             dtype={'geom': Geometry(geom_type, srid= epsg)})    


# In[16]:


#LoadWFS(wfsURL="http://geo.stat.fi/geoserver/vaestoruutu/wfs",layer_name='vaestoruutu:vaki2005_1km_kp',connstr='postgresql://postgres:puntu@localhost:5432/postgres',lyrName='Tekson_WFS',schema='tekson')

