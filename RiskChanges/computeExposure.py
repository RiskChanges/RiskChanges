import pandas as pd
import numpy as np
import numpy.ma as ma
import geopandas as gpd
import warnings
from .RiskChangesOps import rasterops, vectorops, writevector, AggregateData as aggregator
from .RiskChangesOps.readraster import readhaz
from .RiskChangesOps.readvector import readear, readAdmin
from .RiskChangesOps import readmeta


def polygonExposure(ear, haz, expid, Ear_Table_PK):
    df = pd.DataFrame()
    for ind, row in ear.iterrows():
        # print(row)
        # rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        try:
            maska, transform = rasterops.cropraster(haz, [row.geom])
        except:
            continue
        zoneraster = ma.masked_array(maska, mask=maska == 0)
        len_ras = zoneraster.count()
        # print(len_ras)
        if len_ras == 0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique = unique.filled(0)
            idx = np.where(unique == 0)[0][0]
            # print(idx)
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
            # print(len(ids))
            # break
            continue
        elif np.max(ids) == 0:
            continue

        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1] = (frequencies[i][1]/len_ras)*100
        # print(frequencies)
        df_temp = pd.DataFrame(frequencies, columns=['class', 'exposed'])
        df_temp['geom_id'] = row[Ear_Table_PK]
        df_temp['areaOrLen'] = row.geom.area
        df_temp['exposure_id'] = expid
        df = df.append(df_temp, ignore_index=True)

    haz = None
    return df


def lineExposure(ear, haz, expid, Ear_Table_PK):

    gt = haz.transform
    buffersize = gt[0]/4
    df = pd.DataFrame()
    # print(buffersize)
    for ind, row in ear.iterrows():
        polygon = row.geom.buffer(buffersize)
        # rasterio.mask.mask(haz, [row.geometry], crop=True,nodata=0,all_touched=True)
        try:
            maska, transform = rasterops.cropraster(haz, [polygon])
        except:
            continue
        zoneraster = ma.masked_array(maska, mask=maska == 0)
        len_ras = zoneraster.count()
        # print(len_ras)
        if len_ras == 0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique = unique.filled(0)
            idx = np.where(unique == 0)[0][0]
            # print(idx)
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
            # print(len(ids))
            # break
            continue
        elif np.max(ids) == 0:
            continue

        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1] = (frequencies[i][1]/len_ras)*100
        # print(frequencies)
        df_temp = pd.DataFrame(frequencies, columns=['class', 'exposed'])
        df_temp['geom_id'] = row[Ear_Table_PK]
        df_temp['areaOrLen'] = row.geom.length
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
    df_temp['areaOrLen'] = 1
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

    default_cols = ['exposed', 'admin_id', 'class', 'exposure_id', 'areaOrLen']
    if (value_col != None) | (value_col != ''):
        default_cols.append(value_col)
    if (pop_col != None) | (pop_col != ''):
        default_cols.append(pop_col)

    print(geometrytype)
    if (geometrytype == 'Polygon' or geometrytype == 'MultiPolygon'):
        ear['areacheck'] = ear.geom.area
        mean_area = ear.areacheck.mean()
        if mean_area <= 0:
            ear['geom'] = ear['geom'].centroid
            df = pointExposure(ear, haz, expid, Ear_Table_PK)
        else:
            df = polygonExposure(ear, haz, expid, Ear_Table_PK)

    elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
        df = pointExposure(ear, haz, expid, Ear_Table_PK)

    elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
        df = lineExposure(ear, haz, expid, Ear_Table_PK)
    haz = None

    df = pd.merge(left=df, right=ear, left_on='geom_id',
                  right_on=Ear_Table_PK, right_index=False)
    assert not df.empty, f"The aggregated dataframe in exposure returned empty"
    df = gpd.GeoDataFrame(df, geometry='geom')
    # if not onlyaggregated: #due to change of 24 may 2022, it is redundant now because of else statement in coming condition.
    #     df['exposure_id'] = expid
    #     writevector.writeexposure(df, con, schema)

    if is_aggregated:
        admin_unit = readAdmin(con, adminid)
        adminmeta = readmeta.getAdminMeta(con, adminid)
        adminpk = adminmeta.data_id[0]
        admin_unit = gpd.GeoDataFrame(admin_unit, geometry='geom')
        overlaid_Data = gpd.overlay(df, admin_unit[[
                                    adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
        df = overlaid_Data.rename(columns={adminpk: 'admin_id'})

    # if exposure is on individual item, admin_unit must be none
    else:
        df['admin_id'] = ''

    df = df[default_cols]
    df['exposure_id'] = expid

    # default columns for standard database table
    df = df.rename(columns={value_col: "value_exposure",
                            pop_col: "population_exposure"})
    df['areaOrLen'] = df['exposed'] * df['areaOrLen']/100
    df['value_exposure'] = df['exposed'] * df['value_exposure']/100
    df['population_exposure'] = df['exposed'] * df['population_exposure']/100
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
