import pandas as pd
import numpy as np
import numpy.ma as ma
import geopandas as gpd
import warnings
from .RiskChangesOps import rasterops, vectorops, writevector, AggregateData as aggregator
from .RiskChangesOps.readraster import readhaz
from .RiskChangesOps.readvector import readear, readAdmin
from .RiskChangesOps import readmeta
from sqlalchemy import create_engine

# import logging
# logger = logging.getLogger(__file__)

def polygonExposure(ear, haz, expid, Ear_Table_PK):
    df = pd.DataFrame()
    for ind, row in ear.iterrows():
        # rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        try:
            maska, transform,len_ras = rasterops.cropraster(haz, [row.geom])
        except:
            print("could not cropraster")
            continue
        '''
        This code line creates a masked array using the masked_array() function from the numpy.ma module. 
        The masked_array() function takes two arguments: 
        the first argument (maska) is the original array or raster, and 
        The second argument is the mask array, which indicates which elements in the original array should be masked (hidden) from computations
        The mask is set to maska == 0, which means that all elements in maska with a value of zero (which are likely to be nodata values) will be masked out. This effectively removes nodata values from the computation of any statistics or analyses performed on zoneraster.
        '''
        zoneraster = ma.masked_array(maska, mask=maska == 0)
        # len_ras = zoneraster.count()
        # print(len_ras,zoneraster.count(),"len_ras")
        
        if len_ras == 0:
            print("length of raster zero")
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        '''
        The index of the zero value is identified using the where() function from the numpy module, and that index is used to delete the corresponding elements in both the ids and cus arrays using the delete()
        '''
        if ma.is_masked(unique):
            unique = unique.filled(0)
            idx = np.where(unique == 0)[0][0]
            ids = np.delete(unique, idx)
            cus = np.delete(counts, idx)
        else:
            ids = unique
            cus = counts
        if np.isnan(ids).any():
            idx = np.isnan(ids)
            ids = np.delete(ids, idx)
            cus = np.delete(cus, idx)
            
        if len(ids) == 0:
            # print("len of ids zero")
            # df_temp = pd.DataFrame(np.asarray(('', 0)), columns=['class', 'exposed'])
            # df_temp['areaOrLen'] = row.geom.area
            df_temp = pd.DataFrame([[0,0]], columns=['class', 'exposed'])
            df_temp['geom_id'] = row[Ear_Table_PK]
            df_temp['exposure_id'] = expid
            df = df.append(df_temp, ignore_index=True)
            continue
        elif np.max(ids) == 0:
            # print("np max ids zero")
            # df_temp['areaOrLen'] = row.geom.area
            df_temp = pd.DataFrame([[0,0]], columns=['class', 'exposed'])
            df_temp['geom_id'] = row[Ear_Table_PK]
            df_temp['exposure_id'] = expid
            df = df.append(df_temp, ignore_index=True)
            continue
        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1] = (frequencies[i][1]/len_ras)*100
        df_temp = pd.DataFrame(frequencies, columns=['class', 'exposed'])
        df_temp['geom_id'] = row[Ear_Table_PK]
        # df_temp['areaOrLen'] = row.geom.area
        df_temp['exposure_id'] = expid
        df = df.append(df_temp, ignore_index=True)

    haz = None
    return df


def lineExposure(ear, haz, expid, Ear_Table_PK):

    gt = haz.transform
    buffersize = gt[0]/4
    df = pd.DataFrame()
    for ind, row in ear.iterrows():
        polygon = row.geom.buffer(buffersize)
        # rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        try:
            # maska, transform = rasterops.cropraster(haz, [polygon])
            maska, transform,len_ras = rasterops.cropraster(haz, [polygon])
        except:
            continue
        zoneraster = ma.masked_array(maska, mask=maska == 0)
        # len_ras = zoneraster.count()
        if len_ras == 0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique = unique.filled(0)
            idx = np.where(unique == 0)[0][0]
            ids = np.delete(unique, idx)
            cus = np.delete(counts, idx)
        else:
            ids = unique
            cus = counts
        if np.isnan(ids).any():
            idx = np.isnan(ids)
            ids = np.delete(ids, idx)
            cus = np.delete(cus, idx)
        if len(ids) == 0:
            df_temp = pd.DataFrame([[0,0]], columns=['class', 'exposed'])
            df_temp['geom_id'] = row[Ear_Table_PK]
            # df_temp['areaOrLen'] = row.geom.length
            df_temp['exposure_id'] = expid
            df = df.append(df_temp, ignore_index=True)
            continue
        elif np.max(ids) == 0:
            df_temp = pd.DataFrame([[0,0]], columns=['class', 'exposed'])
            df_temp['geom_id'] = row[Ear_Table_PK]
            # df_temp['areaOrLen'] = row.geom.length
            df_temp['exposure_id'] = expid
            df = df.append(df_temp, ignore_index=True)
            continue

        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1] = (frequencies[i][1]/len_ras)*100
        df_temp = pd.DataFrame(frequencies, columns=['class', 'exposed'])
        df_temp['geom_id'] = row[Ear_Table_PK]
        # df_temp['areaOrLen'] = row.geom.length
        df_temp['exposure_id'] = expid
        df = df.append(df_temp, ignore_index=True)
    haz = None
    return df


def pointExposure(ear, haz, expid, Ear_Table_PK):
    coords = [(x, y) for x, y in zip(ear.geometry.x, ear.geometry.y)]
    df_temp = pd.DataFrame()
    classes = []
    for x in haz.sample(coords):
        classes.append(x[0])
    df_temp['class'] = classes
    df_temp['exposure_id'] = expid
    # df_temp['areaOrLen'] = 1
    df_temp['exposed'] = 100
    df_temp['geom_id'] = ear[Ear_Table_PK]
    haz = None
    return df_temp

    # coords = [(x,y) for x, y in zip(ear.geometry.x, ear.geometry.y)]
    # ear['class'] = [x for x in haz.sample(coords)]
    # ear['exposure_id'] = expid
    # ear['areaOrLen'] = 0
    # ear['exposed'] = 100
    # ear=ear.rename(columns={Ear_Table_PK:'geom_id' })
    # haz = None
    # return ear


def ComputeExposure(con, earid, hazid, expid, **kwargs):
    is_aggregated = kwargs.get('is_aggregated', False)
    onlyaggregated = kwargs.get('only_aggregated', True)
    adminid = kwargs.get('adminunit_id', None)
    haz_file = kwargs.get('haz_file', None)

    ear = readear(con, earid)
    haz = readhaz(con, hazid, haz_file)
    # assert vectorops.cehckprojection(
    #    ear, haz), "The hazard and EAR do not have same projection system please check it first"
    if vectorops.cehckprojection(ear, haz):

        warnings.warn("The input co-ordinate system for hazard and EAR were differe, we have updated it for now on the fly but from next time please check your data before computation")
        ear = vectorops.changeprojection(ear, haz)

    metatable = readmeta.earmeta(con, earid)
    Ear_Table_PK = metatable.data_id[0]
    value_col = metatable.col_value_avg[0]
    pop_col = metatable.col_population_avg[0]
    schema = metatable.workspace[0]
    geometrytype = ear.geom_type.unique()[0]

    default_cols = ['exposed', "admin_id", 'class',
                    'exposure_id', 'geom_id','areaOrLength']

    # if value and population column is available, add these to default cols
    # else just add the additional column, we will add null values for these additional cols
    additional_cols = []
    if (value_col != None and value_col != ''):
        default_cols.append(value_col)
    else:
        print('Value colume is not linked!')
        # value_col = 'value_col'
        additional_cols.append('value_col')

    # doing same for population
    if (pop_col != None and pop_col != ''):
        default_cols.append(pop_col)
    else:
        print("population colume is not lined!")
        # pop_col = 'pop_col'
        additional_cols.append('pop_col')

    # check the geometry and run the corresponding calcualtion function
    if (geometrytype == 'Polygon' or geometrytype == 'MultiPolygon'):
        ear['areacheck'] = ear.geom.area
        mean_area = ear.areacheck.mean()
        if mean_area <= 0:
            print("mean area less than zero")
            ear['geom'] = ear['geom'].centroid
            df = pointExposure(ear, haz, expid, Ear_Table_PK)
        else:
            print("call to polygon exposure")
            df = polygonExposure(ear, haz, expid, Ear_Table_PK)
    # point exposure
    elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
        df = pointExposure(ear, haz, expid, Ear_Table_PK)

    # line exposure
    elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
        df = lineExposure(ear, haz, expid, Ear_Table_PK)
    haz = None
    
    assert not df.empty, f"The aggregated dataframe in exposure returned empty, this error may arise if input shapes do not overlap raster"
    df = pd.merge(left=df, right=ear, left_on='geom_id',
                  right_on=Ear_Table_PK, right_index=False)
    df = gpd.GeoDataFrame(df, geometry='geom')
    
    # if not onlyaggregated: #due to change of 24 may 2022, it is redundant now because of else statement in coming condition.
    #     df['exposure_id'] = expid
    #     writevector.writeexposure(df, con, schema)

    if is_aggregated:
        admin_unit = readAdmin(con, adminid)
        adminmeta = readmeta.getAdminMeta(con, adminid)
        adminpk = adminmeta.col_admin[0] or adminmeta.data_id[0]
        admin_unit = gpd.GeoDataFrame(admin_unit, geometry='geom')

        # check whether adminpk and ear columns have same name, issue #80
        df_columns = list(df.columns)
        if adminpk in df_columns:
            df = df.rename(columns={adminpk: f"{adminpk}_ear"})

        overlaid_Data = gpd.overlay(df, admin_unit[[
                                    adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
        df = overlaid_Data.rename(columns={adminpk: 'admin_id'})

    # if exposure is on individual item, admin_unit must be none
    else:
        df['admin_id'] = ''

    df = df[default_cols]
    df['exposure_id'] = expid
    # if value col and pop col are not defined, we assign the value to nan
    # df['areaOrLen_exposure'] = np.nan
    if len(additional_cols) > 0:
        if 'value_col' in additional_cols:
            df['value_exposure'] = np.nan
            df['value_exposure_rel'] = np.nan
            # df[value_col] = np.nan
        if 'pop_col' in additional_cols:
            df['population_exposure'] = np.nan
            df['population_exposure_rel'] = np.nan
            # df[pop_col] = np.nan

    # default columns for standard database table
    # df = df.rename(columns={value_col: "value_exposure",
                            # pop_col: "population_exposure",
                            # })
    df['areaOrLen'] = df['exposed'] * df['areaOrLength']/100  
    if pop_col:
        df['population_exposure'] = df['exposed'] * df[pop_col]/100 
        df['population_exposure_rel'] = np.where(df['population_exposure'] != 0, df['population_exposure'] * 100 / df[pop_col], 0)
    if value_col:
        df['value_exposure'] = df['exposed'] * df[value_col]/100
        df['value_exposure_rel'] = np.where(df['value_exposure'] != 0, df['value_exposure'] * 100 / df[value_col], 0)
    
    if 'areaOrLength' in df.columns:
        df=df.drop('areaOrLength',axis=1)
    if value_col in df.columns:
        df=df.drop(value_col,axis=1)
    if pop_col in df.columns:
        df=df.drop(pop_col,axis=1)
    writevector.writeexposure(df, con, schema)

    # else:
    #     writevector.writeexposure(df, con, schema)

    #************Below is the existing aggregation function written till 24 may 2022. Now we changed it to store in single table**************#
    # if is_aggregated:
    #     admin_unit = readAdmin(con, adminid)
    #     adminmeta = readmeta.getAdminMeta(con, adminid)
    #     adminpk = adminmeta.data_id[0]
    #     df = pd.merge(left=df, right=ear[[
    #                   Ear_Table_PK, 'geom']], left_on='geom_id', right_on=Ear_Table_PK, right_index=False)
    #     assert not df.empty, f"The aggregated dataframe in exposure returned empty"
    #     df = gpd.GeoDataFrame(df, geometry='geom')
    #     df = aggregator.aggregateexpoure(df, admin_unit, adminpk)
    #     assert not df.empty, f"The aggregated dataframe in exposure returned empty"
    #     df['exposure_id'] = expid
    #     df=df[['exposed','admin_id','class','exposure_id','exposed_areaOrLen']]
    #     writevector.writeexposureAgg(df, con, schema)


# kwargs should have an argument for aggregation, admin unit id and save aggregate only or not
#452 and 549
# con="postgresql://postgres:puntu@localhost:5433/SDSSv5"
# df=ComputeExposure(con,549,452,9999)
# writevector.writeexposure(df,con,schema)
