#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#RiskChanges Library
#Exposure Module
#Standardize Hazard Function
#Written by: Ashok Dahal
#For the project: Risk Changes SDSS Development
# Liscence: Creative Commons 
#Written on 22/05/2020


# In[45]:


import gdal
import rasterio
import numpy as np
import ogr
import osr
import os
import sys
import copy


# In[56]:


def reclassify(in_image,out_image,out_dir,classification):
    
    driver = gdal.GetDriverByName('GTiff')
    file = gdal.Open(in_image)
    band = file.GetRasterBand(1)
    lista = band.ReadAsArray()
    classess=classification.keys()
    reclass = copy.copy(lista)
    for key in classess: 
        reclass[np.where(lista<classification[key][1])] = classification[key][0]
    # reclassification
    #for j in  range(file.RasterXSize):
     #   for i in  range(file.RasterYSize):
      #      for key in classess:
       #         if lista[i,j] < classification[key][1]:
        #            lista[i,j] = classification[key][0]
         #           break
            
    export=out_dir+"//"+out_image+".tif"
    # create new file
    file2 = driver.Create( export, file.RasterXSize , file.RasterYSize , 1)
    file2.GetRasterBand(1).WriteArray(lista)

    # spatial ref system
    proj = file.GetProjection()
    georef = file.GetGeoTransform()
    file2.SetProjection(proj)
    file2.SetGeoTransform(georef)
    file2.FlushCache()

