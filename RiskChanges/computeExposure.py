import pandas as pd
import numpy as np
import numpy.ma as ma
import geopandas as gpd

from .RiskChangesOps import rasterops, vectorops, writevector, AggregateData as aggregator
from .RiskChangesOps.readraster import readhaz
from .RiskChangesOps.readvector import readear
from .RiskChangesOps import readmeta

def polygonExposure(ear,haz,expid,Ear_Table_PK):
    df=pd.DataFrame()
    for ind,row in ear.iterrows():
        #print(row)
        maska,transform=rasterops.cropraster(haz,[row.geom])#rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        zoneraster = ma.masked_array(maska, mask=maska==0)
        len_ras=zoneraster.count()
        #print(len_ras)
        if len_ras==0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique=unique.filled(0)
            idx=np.where(unique==0)[0][0]
            #print(idx)
            ids=np.delete(unique, idx)
            cus=np.delete(counts, idx)
        else:
            ids=unique
            cus=counts
        if np.isnan(ids).any():
            idx=np.isnan(ids)
            ids=np.delete(ids, idx)
            cus=np.delete(cus, idx)
        if len(ids)==0:
            #print(len(ids))
            #break
            continue
        elif np.max(ids)==0:
            continue

        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
        #print(frequencies)
        df_temp= pd.DataFrame(frequencies, columns=['class','exposed'])
        df_temp['geom_id']=row[Ear_Table_PK]
        df_temp['areaOrLen']=row.geom.area
        df_temp['exposure_id']=expid
        df=df.append(df_temp,ignore_index=True)
    
    haz=None 
    return df

def lineExposure(ear,haz,expid,Ear_Table_PK):
    
    gt=haz.transform
    buffersize = gt[0]/4
    df=pd.DataFrame()
    #print(buffersize)
    for ind,row in ear.iterrows():
        polygon=row.geom.buffer(buffersize)
        maska,transform=rasterops.cropraster(haz,[polygon])#rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        zoneraster = ma.masked_array(maska, mask=maska==0)
        len_ras=zoneraster.count()
        #print(len_ras)
        if len_ras==0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique=unique.filled(0)
            idx=np.where(unique==0)[0][0]
            #print(idx)
            ids=np.delete(unique, idx)
            cus=np.delete(counts, idx)
        else:
            ids=unique
            cus=counts
        if np.isnan(ids).any():
            idx=np.isnan(ids)
            ids=np.delete(ids, idx)
            cus=np.delete(cus, idx)
        if len(ids)==0:
            #print(len(ids))
            #break
            continue
        elif np.max(ids)==0:
            continue

        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
        #print(frequencies)
        df_temp= pd.DataFrame(frequencies, columns=['class','exposed'])
        df_temp['geom_id']=row[Ear_Table_PK]
        df_temp['areaOrLen']=row.geom.length
        df_temp['exposure_id']=expid
        df=df.append(df_temp,ignore_index=True)
    haz=None 
    return df


def pointExposure(ear,haz,expid,Ear_Table_PK):
    for ind,row in ear.iterrows():
        coords = [(x,y) for x, y in zip(row.geom.x, row.geom.y)]
        a =haz.sample(coords)   
        df_temp['class']= a
        df_temp['exposed']=100
        df_temp['geom_id']=row[Ear_Table_PK]
        df_temp['areaOrLen']=0
        df_temp['exposure_id']=expid
        df=df.append(df_temp,ignore_index=True)
    haz=None 
    return df


def ComputeExposure(con,earid,hazid,expid,**kwargs):
    try:
        is_aggregated=kwargs['is_aggregated']
        onlyaggregated=kwargs['only_aggregated']
        adminid=kwargs['adminunit_id']
    except:
        is_aggregated= False
        onlyaggregated= False
    ear=readear(con,earid)
    haz=readhaz(con,hazid)
    assert vectorops.cehckprojection(ear,haz), "The hazard and EAR do not have same projection system please check it first"
    metatable=readmeta.earmeta(con,earid)
    Ear_Table_PK=metatable.data_id[0]
    schema=metatable.workspace[0]
    geometrytype=ear.geom_type.unique()[0]
    print(geometrytype)
    if (geometrytype== 'Polygon' or geometrytype== 'MultiPolygon'):
        ear['areacheck']=ear.geom.area
        mean_area=ear.areacheck.mean()
        if mean_area<=100:
            ear['geom'] = ear['geom'].centroid
            df= pointExposure(ear,haz,expid,Ear_Table_PK)
        else:
            df= polygonExposure(ear,haz,expid,Ear_Table_PK)
        
    elif(geometrytype=='Point' or geometrytype=='MultiPoint'):
        df= pointExposure(ear,haz,expid,Ear_Table_PK)
        
    elif(geometrytype=='Linestring' or geometrytype=='MultiLinestring'):
        df= lineExposure(ear,haz,expid,Ear_Table_PK)
    haz=None
    if not onlyaggregated:
        writevector.writeexposure(df,con,schema)
    if is_aggregated:
        admin_unit=readear.readAdmin(con,adminid)
        df=pd.merge(left=df, right=ear['id','geom'], left_on='geom_id',right_on='id',right_index=False)
        df= gpd.GeoDataFrame(df,geometry='geom')
        df=aggregator.aggregateexpoure(df,admin_unit)
        writevector.writeexposureAgg(df,con,schema)



    

# kwargs should have an argument for aggregation, admin unit id and save aggregate only or not 
#452 and 549
#con="postgresql://postgres:puntu@localhost:5433/SDSSv5"
#df=ComputeExposure(con,549,452,9999)
#writevector.writeexposure(df,con,schema)