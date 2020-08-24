#!/usr/bin/env python
# coding: utf-8

# In[1]:


#RiskChanges Library
#Exposure Module
#define Hazard Function
#Written by: Ashok Dahal
#For the project: Risk Changes SDSS Development
# Liscence: Creative Commons 
#Written on 22/05/2020


# In[5]:


import gdal
import rasterio
import numpy
import ogr
import osr
import os
import numpy as np


# In[7]:


#supported vector type: Shp, pgtable
#supported raster types: tif

def defineHaz(inhaz,outhaz,outdir,process,param):
    inputHaz=inhaz
    outputHaz=outhaz
    todo=process
    if process=="projectEAR":
        toEPSG=param
        changeProj(inputHaz,toEPSG,outputHaz,outdir)
    if process=="cureNoData":
        replace=param
        curenull(inputHaz,replace,outputHaz,outdir)
        


# In[8]:


def changeProj(inhaz,epsg,outhaz,outdir):
    filename = inhaz
    input_raster = gdal.Open(filename)
    output_raster = outdir+"\\"+outhaz+".tif"
    toEPSG="EPSG:"+str(epsg)
    gdal.Warp(output_raster,input_raster,dstSRS=toEPSG)


# In[14]:


def raster2array(rasterfn):
    raster = gdal.Open(rasterfn)
    band = raster.GetRasterBand(1)
    return band.ReadAsArray()

def getNoDataValue(rasterfn):
    raster = gdal.Open(rasterfn)
    band = raster.GetRasterBand(1)
    return band.GetNoDataValue()

def array2raster(rasterfn,newRasterfn,array):
    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(raster.GetProjectionRef())
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()


# In[18]:


def curenull(inputHaz,replace,outputHaz,outdir):
    rasterfn = inputHaz
    newValue = replace
    newRasterfn = outdir+"\\"+outputHaz+".tif"

    # Convert Raster to array
    rasterArray = raster2array(rasterfn)

    # Get no data value of array
    noDataValue = getNoDataValue(rasterfn)

    # Updata no data value in array with new value
    rasterArray[rasterArray == noDataValue] = newValue

    # Write updated array to new raster
    array2raster(rasterfn,newRasterfn,rasterArray)


