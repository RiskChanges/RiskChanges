#!/usr/bin/env python
# coding: utf-8

# In[1]:

#RiskChanges Library
#Exposure Module
#Define EAR Function
#Written by: Ashok Dahal
#For the project: Risk Changes SDSS Development
# Liscence: Creative Commons 
#Written on 22/05/2020


# In[25]:


import gdal
import rasterio
import numpy
import ogr
import osr
import os


# In[18]:


#supported vector type: Shp, pgtable
#supported raster types: tif

def defineEAR(inear,types,outear,outdir,process,param,conn="None"):
    inputEAR=inear
    outputEAR=outear
    todo=process
    datatype=types
    if process=="projectEAR":
        toEPSG=param
        changeProj(inputEAR,datatype,toEPSG,outear,outdir,conn)
    if process=="addColumn":
        addcolumns=param
        addCol(inputEAR,addcolumns)




def changeProj(inear,types,epsg,outear,outdir,conn):
    if (types=="shp"or types=="pgtable"):
        if (types=="shp"):
            DriverName = "ESRI Shapefile"
            targetref = osr.SpatialReference()
            targetref.ImportFromEPSG(epsg)
            driver = ogr.GetDriverByName(DriverName)
            try:
                dataset = driver.Open(inear)
            except:
                return "Dataset Dosenot Exist"
            inLayer = dataset.GetLayer()
            sourceref = inLayer.GetSpatialRef()
            coordTrans = osr.CoordinateTransformation(sourceref, targetref)
            
            outputShapefile = outdir+"//"+outear+".shp"
            if os.path.exists(outputShapefile):
                driver.DeleteDataSource(outputShapefile)
            outDataSet = driver.CreateDataSource(outputShapefile)
            outLayer = outDataSet.CreateLayer(outear, geom_type=ogr.wkbMultiPolygon)
            inLayerDefn = inLayer.GetLayerDefn()
            
            for i in range(0, inLayerDefn.GetFieldCount()):
                fieldDefn = inLayerDefn.GetFieldDefn(i)
                outLayer.CreateField(fieldDefn)

            # get the output layer's feature definition
            outLayerDefn = outLayer.GetLayerDefn()

            inFeature = inLayer.GetNextFeature()
            while inFeature:
                # get the input geometry
                geom = inFeature.GetGeometryRef()
                # reproject the geometry
                geom.Transform(coordTrans)
                # create a new feature
                outFeature = ogr.Feature(outLayerDefn)
                # set the geometry and attribute
                outFeature.SetGeometry(geom)
                for i in range(0, outLayerDefn.GetFieldCount()):
                    outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))
                # add the feature to the shapefile
                outLayer.CreateFeature(outFeature)
                # dereference the features and get the next input feature
                outFeature = None
                inFeature = inLayer.GetNextFeature()
            inDataSet = None
            outDataSet = None          
        
        elif (types=="pgtable"):
            DriverName="PostgreSQL"
            #print("Updating")
        else:
            print ("Plese provide proper data type name")
        