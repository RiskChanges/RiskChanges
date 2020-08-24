#!/usr/bin/env python
# coding: utf-8

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


# In[4]:


def zonalPoly(feat, lyr, input_value_raster):

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
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geom.Transform(coordTrans)

    # Get extent of feat
    geom = feat.GetGeometryRef()
    if (geom.GetGeometryName() == 'MULTIPOLYGON'):
        count = 0
        pointsX = []; pointsY = []
        for polygon in geom:
            geomInner = geom.GetGeometryRef(count)
            ring = geomInner.GetGeometryRef(0)
            numpoints = ring.GetPointCount()
            for p in range(numpoints):
                    lon, lat, z = ring.GetPoint(p)
                    pointsX.append(lon)
                    pointsY.append(lat)
            count += 1
    elif (geom.GetGeometryName() == 'POLYGON'):
        ring = geom.GetGeometryRef(0)
        numpoints = ring.GetPointCount()
        pointsX = []; pointsY = []
        for p in range(numpoints):
                lon, lat, z = ring.GetPoint(p)
                pointsX.append(lon)
                pointsY.append(lat)

    else:
        sys.exit("ERROR: Geometry needs to be either Polygon or Multipolygon")

    xmin = min(pointsX)
    xmax = max(pointsX)
    ymin = min(pointsY)
    ymax = max(pointsY)

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
    from scipy import stats
    

    # Calculate statistics of zonal raster
    return stats.mode(zoneraster)[0][0][0]


# In[5]:


def zonalPoint(layer,input_value_raster):
    tempDict={}
    featlist=range(lyr.GetFeatureCount())
    raster = gdal.Open(input_value_raster)
    # Get raster georeference info
    gt=raster.GetGeoTransform()
    xOrigin = gt[0]
    yOrigin = gt[3]
    pixelWidth = gt[1]
    pixelHeight = gt[5]
    rb=raster.GetRasterBand(1)
    
    sourceSR = lyr.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromWkt(raster.GetProjectionRef())
    coordTrans = osr.CoordinateTransformation(sourceSR,targetSR)
    
    for FID in featlist:
        feat = lyr.GetFeature(FID)
        geom = feat.GetGeometryRef()
        geom.Transform(coordTrans)
        mx, my = geom.GetX(), geom.GetY()
        px = int((mx - gt[0]) / gt[1])
        py = int((my - gt[3]) / gt[5])
        intval = rb.ReadAsArray(px, py, 1, 1)
        tempDict[FID] = intval[0][0]
    return tempDict


# In[63]:


def zonalLine(layer,input_value_raster):
    tempDict={}
    featlist=range(lyr.GetFeatureCount())
    raster = gdal.Open(input_value_raster)
   
    rb=raster.GetRasterBand(1)
    gt=raster.GetGeoTransform()
    print(gt)
    xOrigin = gt[0]
    yOrigin = gt[3]
    pixelWidth = gt[1]
    pixelHeight = gt[5]
    
    sourceSR = lyr.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromWkt(raster.GetProjectionRef())
    coordTrans = osr.CoordinateTransformation(sourceSR,targetSR)
    print(sourceSR,"and",targetSR)
    
    for FID in featlist:
        feat = lyr.GetFeature(FID)
        geom = feat.GetGeometryRef()
        geom.Transform(coordTrans)
        g = feat.geometry().ExportToWkt()
        print(g)
        shapelyLine = LineString(wkt.loads(g))
        midPoint = shapelyLine.interpolate(shapelyLine.length/2)
        mx, my = midPoint.x,midPoint.y
        px = int((mx - gt[0]) / gt[1])
        py = int((my - gt[3]) / gt[5])
        if px<0:
            px=-px
        if py<0:
            py=py
        print(mx,gt[0],gt[1])
        intval = rb.ReadAsArray(px, py, 1, 1)
        print(intval)
        tempDict[FID] = intval[0][0]
    print("here")
    return tempDict
        


# In[8]:


def loop_zonal_stats(input_zone, input_value_raster):

    shp = ogr.Open(input_zone)
    lyr = shp.GetLayer()
    featList = range(lyr.GetFeatureCount())
    statDict = {}
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
    print(geometrytype)
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        for FID in featList:
            feat = lyr.GetFeature(FID)
            meanValue = zonalpoly(feat, lyr, input_value_raster)
            statDict[FID] = meanValue
        return statDict
    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalPoint(lyr,input_value_raster)
    elif(geometrytype=='LINESTRING'):
        return zonalLine(lyr,input_value_raster)


# In[9]:


def loop_zonal_statsPG(input_zone, input_value_raster,connString):
    from osgeo import ogr
    import sys

    conn = ogr.Open(connString)
    print(conn)
    lyr = conn.GetLayer( input_zone )
    
    featList = range(lyr.GetFeatureCount())
    statDict = {}
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
       
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        for FID in featList:
            feat = lyr.GetFeature(FID)
            meanValue = zonal_statsPG(feat, lyr, input_value_raster)
            statDict[FID] = meanValue
        return statDict
    
    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalPoint(lyr,input_value_raster)
    elif(geometrytype=='LINESTRING'):
        return zonalLine(lyr,input_value_raster)


# In[10]:


def loop_zonal_statsWFS(input_zone,input_value_raster,wfsURL):
    wfs_drv = ogr.GetDriverByName('WFS')

    # Speeds up querying WFS capabilities for services with alot of layers
    gdal.SetConfigOption('OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN', 'NO')

    # Set config for paging. Works on WFS 2.0 services and WFS 1.0 and 1.1 with some other services.
    gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED', 'YES')
    gdal.SetConfigOption('OGR_WFS_PAGE_SIZE', '10000')
    url = 'http://example-service.com/wfs'
    wfs_ds = wfs_drv.Open('WFS:' + wfsURL)
    if not wfs_ds:
        sys.exit('ERROR: can not open WFS datasource')
    else:
        pass
    layer = wfs_ds.GetLayerByName(input_zone)
    if not layer:
        sys.exit('ERROR: can not find layer in service')
    else:
        pass
    statDict = {}
    featList = range(lyr.GetFeatureCount())
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        for FID in featList:
            feat = lyr.GetFeature(FID)
            meanValue = zonal_statsPG(feat, lyr, input_value_raster)
            statDict[FID] = meanValue
        return statDict

    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalPoint(lyr,input_value_raster)
    elif(geometrytype=='LINESTRING'):
        return zonalLine(lyr,input_value_raster)   


# In[11]:


def main(input_zone, input_value_raster,earsource,**kwargs):
    if earsource=="shp":
        return loop_zonal_stats(input_zone, input_value_raster)
        
    elif earsource =="pgtable":
        connString = kwargs.get('connString', "None")
        if(connString=="None"):
            print("Please Supply valid connection string")
        print("please supply valid data and their source")
        return loop_zonal_statsPG(input_zone, input_value_raster,connString)
    elif earsource=="wfs":
        wfsURL=kwargs.get('wfsURL', "None")
        if(wfsURL=="None"):
            return("Please provide valid WFS URL")
        return loop_zonal_statsWFS(input_zone,input_value_raster,wfsURL)
    else:
        print("please supply valid data and their source")
        
#'''    databaseServer = "127.0.0.1"
#    databaseName = "postgres"
#    databaseUser = "ashok"
#    databasePW = "gicait123"
#    connString = "PG: host=%s dbname=%s user=%s password=%s" % (databaseServer,databaseName,databaseUser,databasePW)
#'''


# In[ ]:





# In[ ]:




