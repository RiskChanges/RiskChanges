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
from rasterio.io import MemoryFile
import rasterio
from rasterio.enums import Resampling
# from rasterio.coords import disjoint_bounds
# from rasterio.transform import from_origin
# from rasterio.windows import Window
# from collections import Counter

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

def ComputeRasterExposure(con, earid, hazid, expid, **kwargs):
    try:
        metatable = readmeta.earmeta(con, earid)
        schema = metatable.workspace[0]
        adminid = kwargs.get('adminunit_id', None)
        haz_file = kwargs.get('haz_file', None)
        ear_file = kwargs.get('ear_file', None)

        hazard_raster = readhaz(con, hazid, haz_file)
        ear_raster = rasterio.open(ear_file) #handle this

        final_x_res=int(ear_raster.res[0])
        final_y_res=int(ear_raster.res[1])
        resampled_haz_width=int((hazard_raster.width * hazard_raster.res[0]) / final_x_res)
        resampled_haz_height=int((hazard_raster.height * hazard_raster.res[1]) / final_y_res)
        resampled_ear_width=int((ear_raster.width * ear_raster.res[0]) / final_x_res)
        resampled_ear_height=int((ear_raster.height * ear_raster.res[1]) / final_y_res)
        
        # Resample the source raster to match the target resolution
        resampled_haz_data = hazard_raster.read(
            out_shape=(hazard_raster.count, resampled_haz_height, resampled_haz_width),
            resampling=Resampling.nearest
        )
        resampled_ear_data = ear_raster.read(
            out_shape=(ear_raster.count, resampled_ear_height, resampled_ear_width),
            resampling=Resampling.nearest
        )
        
        # Get x and y origin based on the original transformation
        haz_x_origin, haz_y_origin = hazard_raster.transform[2],hazard_raster.transform[5] 
        ear_x_origin, ear_y_origin = ear_raster.transform[2],ear_raster.transform[5] 

        # Create a new Affine transformation with the updated pixel dimensions
        new_haz_transform = rasterio.transform.Affine(final_x_res,0.0, haz_x_origin,0.0,-final_y_res, haz_y_origin)
        new_ear_transform = rasterio.transform.Affine(final_x_res,0.0, ear_x_origin,0.0,-final_y_res, ear_y_origin)

        resampled_haz_meta = {
            'driver': 'GTiff',
            'dtype': resampled_haz_data.dtype,
            'count': 1,  # Number of bands
            'height': resampled_haz_height,
            'width': resampled_haz_width,
            'crs': hazard_raster.crs,  # Replace with your desired CRS
            'transform': new_haz_transform
        }
        resampled_ear_meta = {
            'driver': 'GTiff',
            'dtype': resampled_ear_data.dtype,
            'count': 1,  # Number of bands
            'height': resampled_ear_height,
            'width': resampled_ear_width,
            'crs': ear_raster.crs,  # Replace with your desired CRS
            'transform': new_ear_transform
        }

        with MemoryFile() as memfile:
            with memfile.open(**resampled_haz_meta) as dst:
                dst.write(resampled_haz_data)
            resampled_hazard_raster = memfile.open()
            
        with MemoryFile() as memfile:
            with memfile.open(**resampled_ear_meta) as dst:
                dst.write(resampled_ear_data)
            resampled_ear_raster = memfile.open()

        hazard_raster_data = resampled_hazard_raster.read(1)  # Read the first band of raster 1
        ear_raster_data = resampled_ear_raster.read(1)  # Read the first band of raster 2

        hazard_bounds = resampled_hazard_raster.bounds #Get the extent of hazard dataset
        ear_bounds = resampled_ear_raster.bounds #Get the extent of hazard dataset
        
        # Calculate intersection bounds
        intersection_bounds = (
            max(ear_bounds.left, hazard_bounds.left),
            max(ear_bounds.bottom, hazard_bounds.bottom),
            min(ear_bounds.right, hazard_bounds.right),
            min(ear_bounds.top, hazard_bounds.top)
        )
        
        hazard_window = resampled_hazard_raster.window(*intersection_bounds)
        hazard_row_off,hazard_col_off = int(hazard_window.row_off), int(hazard_window.col_off)
        hazard_height,hazard_width = int(hazard_window.height), int(hazard_window.width)

        ear_window = resampled_ear_raster.window(*intersection_bounds)
        ear_row_off, ear_col_off = int(ear_window.row_off), int(ear_window.col_off)
        ear_height,ear_width  = int(ear_window.height), int(ear_window.width)
        
        clipped_hazard_raster = hazard_raster_data[hazard_row_off:(hazard_row_off + hazard_height), hazard_col_off:(hazard_col_off + hazard_width)]
        clipped_ear_raster = ear_raster_data[ear_row_off:(ear_row_off + ear_height), ear_col_off:(ear_col_off + ear_width)]

        ear_x_origin = resampled_ear_raster.transform[2]+(ear_col_off*final_x_res)
        ear_y_origin = resampled_ear_raster.transform[5]+(ear_row_off*final_y_res)

        hazard_x_origin = resampled_hazard_raster.transform[2]+(hazard_col_off*final_x_res)
        hazard_y_origin = resampled_hazard_raster.transform[5]+(hazard_row_off*final_y_res)

        #handeling nodata and nan value in hazard datasets
        has_nodata = np.isnan(clipped_hazard_raster).any()  
        if has_nodata:
            clipped_hazard_raster = np.nan_to_num(clipped_hazard_raster, nan=0.0)
        nodata = resampled_hazard_raster.nodata
        clipped_hazard_raster[clipped_hazard_raster == nodata]=0.0
        clipped_hazard_raster = clipped_hazard_raster[clipped_hazard_raster != nodata] 
        
        #handeling nodata and nan value in ear datasets
        has_nodata = np.isnan(clipped_ear_raster).any()  
        if has_nodata:
            clipped_ear_raster = np.nan_to_num(clipped_ear_raster, nan=0.0)
        nodata = resampled_ear_raster.nodata
        clipped_ear_raster[clipped_ear_raster == nodata]=0.0
        clipped_ear_raster = clipped_ear_raster[clipped_ear_raster != nodata] 
        
        #Get unique pixel value in hazard and ear datasets
        unique_ear_pixel_values=np.unique(clipped_ear_raster)
        unique_hazard_pixel_values=np.unique(clipped_hazard_raster)
        
        # pivot_data = pd.DataFrame(index=unique_ear_pixel_values, columns=unique_hazard_pixel_values, dtype=int)
        # # print("pivot data")
        # # # Fill the pivot table with counts
        # for hazard_value in unique_hazard_pixel_values:
        #     print(hazard_value,"hazard_value")
        #     for ear_value in unique_ear_pixel_values:
        #         count = np.sum((clipped_hazard_raster == hazard_value) & (clipped_ear_raster == ear_value))
        #         pivot_data.at[ear_value, hazard_value] = count
        # print(pivot_data)
        
        #create empty dataframe
        df = pd.DataFrame(columns=["hazard_name", "ear_name", "total_pixel_exposed","total_area_exposed","relative_exposed"])
        total_pixel_count=ear_height*ear_width
        
        for hazard_value in unique_hazard_pixel_values:
            for ear_value in unique_ear_pixel_values:
                total_pixel_exposed = np.sum((clipped_hazard_raster == hazard_value) & (clipped_ear_raster == ear_value))
                total_area_exposed=total_pixel_exposed*final_x_res*final_y_res
                relative_exposed=total_pixel_exposed*100/total_pixel_count
                df = df.append({
                        "hazard_name": hazard_value,
                        "ear_name": ear_value, 
                        "total_pixel_exposed": total_pixel_exposed,
                        "total_area_exposed":total_area_exposed,
                        "relative_exposed":round(relative_exposed,3),
                        }, ignore_index=True)
                
        table_name="raster_exposure_result"
        df['exposure_id'] = expid
        df['admin_id'] = None
        # print(df)
        writevector.writeexposure(df, con, schema,table_name)
        print("done")
    except Exception as e:
        print("error ",str(e))
        
    