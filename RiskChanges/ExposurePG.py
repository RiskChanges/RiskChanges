#!/usr/bin/env python
# coding: utf-8

# In[87]:


import gdal
import numpy
import ogr
import osr
import os
import sys
import pandas as pd
import numpy as np
import copy
#from sqlalchemy import *
import pandas as pd
import re
from sqlalchemy import create_engine


def zonalPoly(feat, lyr, input_value_raster):
    import gdal
    # Open data
    raster = gdal.Open(input_value_raster)
    # Get raster georeference info
    transform = raster.GetGeoTransform()
    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = transform[5]
    # Reproject vector geometry to same projection as raster
    sourceSR = lyr.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromWkt(raster.GetProjectionRef())
    coordTrans = osr.CoordinateTransformation(sourceSR,targetSR)
    #feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geom.Transform(coordTrans)
    # Get extent of feat
    geom = feat.GetGeometryRef()
    extent = geom.GetEnvelope()
    #print(extent)
    ###### TODO::: create xmin xmax y min ymax from getEnvelope 
    xmin = extent[0]
    xmax = extent[1]
    ymin = extent[2]
    ymax = extent[3]

    # Specify offset and rows and columns to read
    xoff = int((xmin - xOrigin)/pixelWidth)
    yoff = int((yOrigin - ymax)/pixelWidth)
    xcount = int((xmax - xmin)/pixelWidth)+1
    ycount = int((ymax - ymin)/pixelWidth)+1

    # Create memory target raster
    target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, 1, gdal.GDT_Byte)
    target_ds.SetGeoTransform((
        xmin, pixelWidth, 0,
        ymax, 0, pixelHeight,
    ))
    # Create for target raster the same projection as for the value raster
    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromWkt(raster.GetProjectionRef())
    target_ds.SetProjection(raster_srs.ExportToWkt())

    # Rasterize zone polygon to raster
    gdal.RasterizeLayer(target_ds, [1], lyr, burn_values=[1])

    # Read raster as arrays
    banddataraster = raster.GetRasterBand(1)
    dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

    bandmask = target_ds.GetRasterBand(1)
    datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)
    # Mask zone of raster
    zoneraster = numpy.ma.masked_array(dataraster,  numpy.logical_not(datamask))
   #print(zoneraster)
    (unique, counts) = numpy.unique(zoneraster, return_counts=True)
    unique[unique.mask] = 9999
    if 9999 in unique:
        falsedata=numpy.where(unique==9999)[0][0]
        ids=numpy.delete(unique, falsedata)
        cus=numpy.delete(counts, falsedata)
    else:
        ids=unique
        cus=counts
    #print(ids)
    frequencies = numpy.asarray((ids, cus)).T
    len_ras=zoneraster.count()
    for i in range(len(frequencies)):
        frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
    #print(frequencies)
    # Calculate statistics of zonal raster
    return frequencies


# In[77]:


#lead co-ordinate system should be EAR
def zonalPoint(lyr,input_value_raster,exposure_id,Ear_Table_PK,agg_col):
    tempDict={}
    featlist=range(lyr.GetFeatureCount())
    raster = gdal.Open(input_value_raster)
    
    projras = osr.SpatialReference(wkt=raster.GetProjection())
    epsgras=projras.GetAttrValue('AUTHORITY',1)
    
    projear=lyr.GetSpatialRef()
    epsgear=projear.GetAttrValue('AUTHORITY',1)
    print(epsgear,epsgras)
    if not epsgras==epsgear:
        toEPSG="EPSG:"+str(epsgear)
        output_raster=input_value_raster.replace(".tif","_projected.tif")
        gdal.Warp(output_raster,input_value_raster,dstSRS=toEPSG)
        raster=None
        raster=gdal.Open(output_raster)
    else:
        pass
    # Get raster georeference info
    
    gt=raster.GetGeoTransform()
    xOrigin = gt[0]
    yOrigin = gt[3]
    pixelWidth = gt[1]
    pixelHeight = gt[5]
    rb=raster.GetRasterBand(1)
    

    df = pd.DataFrame()
    for FID in featlist:
        if agg_col is  None :
            FID+=1
        print(FID)
        feat = lyr.GetFeature(FID)
        geom = feat.GetGeometryRef()
        mx, my = geom.GetX(), geom.GetY()
        px = int((mx - gt[0]) / gt[1])
        py = int((my - gt[3]) / gt[5])
        intval = rb.ReadAsArray(px, py, 1, 1)
        df_temp= pd.DataFrame([[intval[0][0]]], columns=['class'])
        df_temp['geom_id'] = FID
        df_temp['exposure_id'] = exposure_id
        #df_temp['class'] = intval[0][0]
        df_temp['exposed']=100
        print(df_temp)
        if agg_col is not None :
            df_temp['admin_unit'] =feat[agg_col]            
        df=df.append(df_temp,ignore_index=True)
        
    return df


# In[78]:


def zonalLine(feat, lyr, input_value_raster):
    raster = gdal.Open(input_value_raster)
    
    # Get raster georeference info
    transform = raster.GetGeoTransform()
    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = transform[5]

    # Reproject vector geometry to same projection as raster
    sourceSR = lyr.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromWkt(raster.GetProjectionRef())
    coordTrans = osr.CoordinateTransformation(sourceSR,targetSR)
    #feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geom.Transform(coordTrans)

    # Get extent of feat
   
    geom = feat.GetGeometryRef()
    extent = geom.GetEnvelope()
    #print(extent)
    ###### TODO::: create xmin xmax y min ymax from getEnvelope 
    xmin = extent[0]
    xmax = extent[1]
    ymin = extent[2]
    ymax = extent[3]

    # Specify offset and rows and columns to read
    xoff = int((xmin - xOrigin)/pixelWidth)
    yoff = int((yOrigin - ymax)/pixelWidth)
    xcount = int((xmax - xmin)/pixelWidth)+1
    ycount = int((ymax - ymin)/pixelWidth)+1

    # Create memory target raster
    target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, 1, gdal.GDT_Byte)
    target_ds.SetGeoTransform((
        xmin, pixelWidth, 0,
        ymax, 0, pixelHeight,
    ))

    # Create for target raster the same projection as for the value raster
    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromWkt(raster.GetProjectionRef())
    target_ds.SetProjection(raster_srs.ExportToWkt())

    # Rasterize zone polygon to raster
    gdal.RasterizeLayer(target_ds, [1], lyr, burn_values=[1])

    # Read raster as arrays
    banddataraster = raster.GetRasterBand(1)
    dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

    bandmask = target_ds.GetRasterBand(1)
    datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)

    # Mask zone of raster
    zoneraster = numpy.ma.masked_array(dataraster,  numpy.logical_not(datamask))
   #print(zoneraster)
    (unique, counts) = numpy.unique(zoneraster, return_counts=True)
    unique[unique.mask] = 9999
    if 9999 in unique:
        falsedata=numpy.where(unique==9999)[0][0]
        ids=numpy.delete(unique, falsedata)
        cus=numpy.delete(counts, falsedata)
    else:
        ids=unique
        cus=counts
    frequencies = numpy.asarray((ids, cus)).T
    len_ras=zoneraster.count()
    for i in range(len(frequencies)):
        frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
    #print(frequencies)
    # Calculate statistics of zonal raster
    return frequencies


# In[79]:


def ExposurePgAg(input_zone,admin_unit,agg_col, input_value_raster,connString,exposure_id,Ear_Table_PK):  
    conn = ogr.Open(connString)
    sql='SELECT '+input_zone+'.*,'+admin_unit+'.'+agg_col +' FROM '+ input_zone+' , '+admin_unit+' WHERE ST_Intersects( '+ input_zone+'.geom , '+admin_unit+'.geom )'
    lyr=conn.ExecuteSQL(sql)    
    featList = range(lyr.GetFeatureCount())
    statDict = {}
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
    print(geometrytype)
    i=1
    df = pd.DataFrame()
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        for FID in featList:
            print("progress",((FID*100)/lyr.GetFeatureCount()),"%")
            feat = lyr.GetFeature(FID)
            area = feat.GetGeometryRef().GetArea()
            exposurd = zonalPoly(feat, lyr, input_value_raster)
            df_temp= pd.DataFrame(exposurd, columns=['class','exposed'])
            df_temp['geom_id'] = feat[Ear_Table_PK]
            df_temp['exposure_id'] = exposure_id
            df_temp['admin_unit'] =feat[agg_col]
            df_temp['areaOrLen']=area
            df=df.append(df_temp,ignore_index=True)
        return df
    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalPoint(lyr,input_value_raster,exposure_id,Ear_Table_PK,agg_col)
    elif(geometrytype=='LINESTRING' or geometrytype=='MULTILINESTRING'):
        for FID in featList:
            print("progress",((FID*100)/lyr.GetFeatureCount()),"%")
            feat = lyr.GetFeature(FID)
            length = feat.GetGeometryRef().Length()
            exposurd = zonalLine(feat, lyr, input_value_raster)
            df_temp= pd.DataFrame(exposurd, columns=['class','exposed'])
            df_temp['geom_id'] = feat[Ear_Table_PK]
            df_temp['exposure_id'] = exposure_id
            df_temp['admin_unit'] =feat[agg_col]
            df_temp['areaOrLen']=length
            df=df.append(df_temp,ignore_index=True)
        return df


# In[139]:


def ExposurePG(input_zone, input_value_raster,connString,exposure_id,Ear_Table_PK):
    print(input_zone)
    conn = ogr.Open(connString)
    lyr =conn.GetLayer(input_zone)
    featList = range(lyr.GetFeatureCount())
    print(featList)
    statDict = {}
    feat = lyr.GetNextFeature()
    print(lyr.GetLayerDefn())
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
    print(geometrytype)
    i=1
    df = pd.DataFrame()
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        for FID in featList:
            FID+=1
            print("progress",((FID*100)/lyr.GetFeatureCount()),"%")
            feat = lyr.GetFeature(FID)
            exposurd = zonalPoly(feat, lyr, input_value_raster)
            df_temp= pd.DataFrame(exposurd, columns=['class','exposed'])
            #print(feat['bu'],Ear_Table_PK)
            df_temp['geom_id'] = feat[Ear_Table_PK]
            df_temp['exposure_id'] = exposure_id
            #df_temp['admin_unit'] =feat[agg_col]
            df=df.append(df_temp,ignore_index=True)
            #print(df)
        return df
    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalPoint(lyr,input_value_raster,exposure_id,Ear_Table_PK,agg_col=None)
    elif(geometrytype=='LINESTRING' or geometrytype=='MULTILINESTRING'):
        for FID in featList:
            FID+=1
            print("progress",((FID*100)/lyr.GetFeatureCount()),"%")
            feat = lyr.GetFeature(FID)
            exposurd = zonalLine(feat, lyr, input_value_raster)
            df_temp= pd.DataFrame(exposurd, columns=['class','exposed'])
            df_temp['geom_id'] = feat[Ear_Table_PK]
            df_temp['exposure_id'] = exposure_id
            #df_temp['admin_unit'] =feat[agg_col]
            df=df.append(df_temp,ignore_index=True)
            #print(df)
        return df


# In[81]:


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


# In[82]:


def aggregate(df,agg_col):
    try:
        df['exposed_areaOrLen']=df['exposed']*df['areaOrLen']/100
        df_aggregated=df.groupby([agg_col,'class'],as_index=False).agg({'exposed_areaOrLen':'sum','exposed':'count'})
    except:
        df_aggregated=df.groupby(['admin_unit','class'],as_index=False).agg({'exposed':'count','exposure_id':'mean'})
    return df_aggregated


# In[122]:


def todatabase(df,connstr,table_name,schema):  
  # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)
    
    #... [do something with the geodataframe]

   # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'
    try:
        df.to_sql(table_name, engine, schema,if_exists='append', index=False)
    except:
        return("error, trying to append in non related table,please store in same table as EAR")
    engine.dispose()


# In[126]:


def CalculateExposure(Ear_Table, Ear_Table_PK,haz_dir,connString,
                      connSchema,exposure_id,exposure_table,
                      admin_unit=None,agg_col=None,aggregation=False): 
    a=re.split(':|//|/|@',connString)
    databaseServer=a[4]
    databaseName=a[6]
    databaseUser=a[2]
    databasePW=a[3]
    connStringOGR = "PG: host=%s dbname=%s user=%s password=%s  schemas=%s" % (databaseServer,databaseName,databaseUser,databasePW,connSchema)
    input_zone=Ear_Table
    input_value_raster=haz_dir
    
    if aggregation is True:
        if admin_unit is not None:
            df=ExposurePgAg(input_zone,admin_unit,agg_col, input_value_raster,connStringOGR,exposure_id,Ear_Table_PK)
            df_aggregated=aggregate(df,agg_col)
            todatabase(df,connString,exposure_table,connSchema)
            todatabase(df_aggregated,connString,exposure_table+'_agg',connSchema)
            return ('completed')
        elif admin_unit is None:
            return ("Please provide the admin unit and aggregation column for the aggregation")
    else:
        df=ExposurePG(input_zone, input_value_raster,connStringOGR,exposure_id,Ear_Table_PK)
        todatabase(df,connString,exposure_table,connSchema)
        return ('completed')
