#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Load WCS Module


# In[3]:


import gdal
import rasterio
import numpy
import ogr
import osr
import os
import sys
from rasterstats import zonal_stats
from shapely.geometry import LineString
from shapely import wkt
from owslib.wcs import WebCoverageService 


# In[18]:


def loadWCS(folder,out_name,WCS_URL,Version,layerName,bbox,srid):
    folder = folder
    urls=WCS_URL
    version=Version
    bbox=bbox
    input_value_raster=layerName
    crs_num='urn:ogc:def:crs:EPSG::'+str(srid)

    if urls=='None'  :
        return("Please provide WCS parameters")

    wcs = WebCoverageService(urls, version)
    #print(list(wcs.contents))

    #
    # print([op.name for op in wcs.operations])

    cvg = wcs.contents[input_value_raster]
    if bbox=='None':
        bbox=cvg.boundingBoxWGS84
        
    response = wcs.getCoverage(identifier=input_value_raster, bbox=bbox, format='GEOTIFF_FLOAT32', crs='urn:ogc:def:crs:EPSG::28992', resx=0.5, resy=0.5)   
    temp_raster=folder+'//'+out_name+'.tif'
    with open(temp_raster, 'wb') as file:
        file.write(response.read())
    
    


# In[19]:


#x, y = 174100, 444100
#bbox = (x-500, y-500, x+500, y+500)
#loadWCS(folder="D:/",out_name='ashok_WCS',WCS_URL='http://geodata.nationaalgeoregister.nl/ahn2/wcs?service=WCS',Version='1.0.0',layerName='ahn2_05m_ruw',bbox=bbox,srid=28992)


# In[1]:




