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

def polygonExposure(ear, haz, expid, Ear_Table_PK):
    df = pd.DataFrame()
    for ind, row in ear.iterrows():
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
            # df_temp = pd.DataFrame(np.asarray(('', 0)), columns=['class', 'exposed'])
            # df_temp['areaOrLen'] = row.geom.area
            df_temp = pd.DataFrame([[0,0]], columns=['class', 'exposed'])
            df_temp['geom_id'] = row[Ear_Table_PK]
            df_temp['exposure_id'] = expid
            df = df.append(df_temp, ignore_index=True)
            continue
        elif np.max(ids) == 0:
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
    print("*******************1111111111111111111")
    print(Ear_Table_PK,"Ear_Table_PK")
    print("*******************1111111111111111111")
    
    gt = haz.transform
    buffersize = gt[0]/4
    df = pd.DataFrame()
    for ind, row in ear.iterrows():
        try:
            polygon = row.geom.buffer(buffersize)
        except:
            polygon = row.geometry.buffer(buffersize)
            
        try:
            maska, transform,len_ras = rasterops.cropraster(haz, [polygon])
        except:
            continue
        zoneraster = ma.masked_array(maska, mask=maska == 0)
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
        df_temp['areaOrLength'] = row['areaOrLength']
        # df_temp['areaOrLen'] = row.geom.length
        df_temp['exposure_id'] = expid
        df = df.append(df_temp, ignore_index=True)
    haz = None
    return df


def pointExposure(ear, haz, expid, Ear_Table_PK):
    coords = [(x, y) for x, y in zip(ear.geometry.x, ear.geometry.y)]
    df_temp = pd.DataFrame()
    classes = []
    exposed_values = []
    for x in haz.sample(coords):
        if int(x[0]) == -999:
            classes.append(0)
            exposed_values.append(0)
        else:
            classes.append(x[0])
            exposed_values.append(100)
    df_temp['class'] = classes
    df_temp['exposed'] = exposed_values
    df_temp['exposure_id'] = expid
    df_temp['geom_id'] = ear[Ear_Table_PK]
    # df_temp['areaOrLen'] = 1
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
        # print('Value colume is not linked!')
        # value_col = 'value_col'
        additional_cols.append('value_col')

    # doing same for population
    if (pop_col != None and pop_col != ''):
        default_cols.append(pop_col)
    else:
        # pop_col = 'pop_col'
        additional_cols.append('pop_col')

    # if is_aggregated:
    #     admin_unit = readAdmin(con, adminid)
    #     adminmeta = readmeta.getAdminMeta(con, adminid)
    #     adminpk = adminmeta.col_admin[0] or adminmeta.data_id[0]
    #     admin_unit = gpd.GeoDataFrame(admin_unit, geometry='geom')

    #     # check whether adminpk and ear columns have same name, issue #80
    #     df_columns = list(ear.columns)
    #     if adminpk in df_columns:
    #         ear = ear.rename(columns={adminpk: f"{adminpk}_ear"})
    #     print(len(ear),"len ear beforeeeeeeeee")

    #     overlaid_Data = gpd.overlay(ear, admin_unit[[
    #                                 adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)

    #     # overlaid_Data_false = gpd.overlay(ear, admin_unit[[
    #     #                             adminpk, 'geom']], how='intersection', make_valid=False, keep_geom_type=True)
    #     ear = overlaid_Data.rename(columns={adminpk: 'admin_id'})
    #     ear = ear.rename(columns={"geometry": "geom"})
    #     ear = gpd.GeoDataFrame(ear, geometry='geom')
    #     print(len(ear),"len ear afterrrrrrrrr")
    #     # print(len(overlaid_Data_false),"length overlaid_Data_falseeeeeeee")
    # # if exposure is on individual item, admin_unit must be none
    # else:
    #     ear['admin_id'] = ''
    df=ear
    if is_aggregated:
        admin_unit = readAdmin(con, adminid)
        adminmeta = readmeta.getAdminMeta(con, adminid)
        adminpk = adminmeta.col_admin[0] or adminmeta.data_id[0]
        admin_unit = gpd.GeoDataFrame(admin_unit, geometry='geom')
        # check whether adminpk and ear columns have same name, issue #80
        df_columns = list(df.columns)
        if adminpk in df_columns:
            df = df.rename(columns={adminpk: f"{adminpk}_ear"})
        temp_df=df
        if (geometrytype=='Polygon' or geometrytype == 'MultiPolygon'):
            temp_df['centroid'] = temp_df.centroid
            temp_centroid_df=gpd.GeoDataFrame(temp_df, geometry='centroid')

            overlaid_Data = gpd.overlay(temp_centroid_df, admin_unit[[
                                    adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
            # overlaid_Data = gpd.overlay(df, admin_unit[[
            #                             adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
            df = overlaid_Data.rename(columns={adminpk: 'admin_id'})
        elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
            overlaid_Data = gpd.overlay(df, admin_unit[[
                                    adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
            df = overlaid_Data.rename(columns={adminpk: 'admin_id'})
        elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
            temp_df['areaOrLen_original']=temp_df.geometry.length.round(3)
            overlaid_Data = gpd.overlay(temp_df, admin_unit[[
                                    adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
            overlaid_Data['areaOrLen_overlaid']=overlaid_Data.geometry.length.round(3)
            non_zero_length_condition=(overlaid_Data['areaOrLen_overlaid']!=0.000)
            overlaid_Data=overlaid_Data[non_zero_length_condition]
            overlaid_Data['occupied_percent']=overlaid_Data['areaOrLen_overlaid']*100/overlaid_Data['areaOrLen_original']
            overlaid_Data=overlaid_Data.drop('areaOrLen_original',axis=1)
            overlaid_Data=overlaid_Data.drop('areaOrLength',axis=1)
            df = overlaid_Data.rename(columns={adminpk: 'admin_id','areaOrLen_overlaid':'areaOrLength',"geometry":"geom"})
    # if exposure is on individual item, admin_unit must be none
    else:
        df['admin_id'] = ''
        
        
    # # check the geometry and run the corresponding calcualtion function
    # if (geometrytype == 'Polygon' or geometrytype == 'MultiPolygon'):
    #     ear['areacheck'] = ear.geom.area
    #     mean_area = ear.areacheck.mean()
    #     if mean_area <= 0:
    #         ear['geom'] = ear['geom'].centroid
    #         df = pointExposure(ear, haz, expid, Ear_Table_PK)
    #     else:
    #         df = polygonExposure(ear, haz, expid, Ear_Table_PK)
    # # point exposure
    # elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
    #     df = pointExposure(ear, haz, expid, Ear_Table_PK)

    # # line exposure
    # elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
    #     df = lineExposure(ear, haz, expid, Ear_Table_PK)
    # haz = None
    # assert not df.empty, f"The aggregated dataframe in exposure returned empty, this error may arise if input shapes do not overlap raster"
    # df = pd.merge(left=df, right=ear, left_on='geom_id',
    #               right_on=Ear_Table_PK, right_index=False)
    # df = gpd.GeoDataFrame(df, geometry='geom')
    
    # check the geometry and run the corresponding calcualtion function
    if (geometrytype == 'Polygon' or geometrytype == 'MultiPolygon'):
        df['areacheck'] = df.geom.area
        mean_area = df.areacheck.mean()
        if mean_area <= 0:
            df['geom'] = df['geom'].centroid
            exp_df = pointExposure(df, haz, expid, Ear_Table_PK)
        else:
            exp_df = polygonExposure(df, haz, expid, Ear_Table_PK)
    # point exposure
    elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
        exp_df = pointExposure(df, haz, expid, Ear_Table_PK)

    # line exposure
    elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
        exp_df = lineExposure(df, haz, expid, Ear_Table_PK)
        
    haz = None
    assert not exp_df.empty, f"The aggregated dataframe in exposure returned empty, this error may arise if input shapes do not overlap raster"
    
    if (geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
    
        df = pd.merge(left=exp_df, right=df, left_on=['geom_id','areaOrLength'],
                    right_on=[Ear_Table_PK,'areaOrLength'], right_index=False)
    else:
        df = pd.merge(left=exp_df, right=df, left_on='geom_id',
                    right_on=Ear_Table_PK, right_index=False)
    
    df = gpd.GeoDataFrame(df)
    # df = gpd.GeoDataFrame(df, geometry='geom')
    
    
    # print(len(df),"len ear after merge")
    # print(df.columns," ear columns after merge")
    
    # if not onlyaggregated: #due to change of 24 may 2022, it is redundant now because of else statement in coming condition.
    #     df['exposure_id'] = expid
    #     writevector.writeexposure(df, con, schema)


    df = df[default_cols]
    df['exposure_id'] = expid
    # if value col and pop col are not defined, we assign the value to nan
    # df['areaOrLen_exposure'] = np.nan
    # df[value_col] = np.nan
    # df[pop_col] = np.nan
    if len(additional_cols) > 0:
        if 'value_col' in additional_cols:
            df['value_exposure'] = np.nan
            df['value_exposure_rel'] = np.nan
        if 'pop_col' in additional_cols:
            df['population_exposure'] = np.nan
            df['population_exposure_rel'] = np.nan

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
    df['count']=np.where(df['exposed'] == 0, 0, 1)
    df['count_rel']=np.where(df['exposed'] == 0, 0, 100)    
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

'''
rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
len_ras = zoneraster.count()
print(len_ras,zoneraster.count(),"len_ras")
rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
maska, transform = rasterops.cropraster(haz, [polygon])
len_ras = zoneraster.count()
assert vectorops.cehckprojection(
   ear, haz), "The hazard and EAR do not have same projection system please check it first"
''' 
