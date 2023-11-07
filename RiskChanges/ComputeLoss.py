import geopandas as gpd
import pandas as pd
import numpy as np

from .RiskChangesOps.readvulnerability import readIntVuln, readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator


def getHazardMeanIntensity(exposuretable, stepsize, base, threshold):
    stepsize = stepsize  # 5 #import from database
    base = base  # 0 #import from database
    half_step = stepsize/2
    max_intensity = threshold
    max_class = exposuretable['class'].max()
    exposuretable['meanHazard'] = base + \
        exposuretable['class']*stepsize-half_step
    exposuretable.loc[exposuretable['class'] ==
                      max_class, 'meanHazard'] = max_intensity
    exposuretable['meanHazard']
    return exposuretable


def estimatevulnerability(exposuretable, haztype, vulnColumn, con):
    #!not required I guess
    if haztype == "Susceptibility not relevant":
        for i in exposuretable[vulnColumn].unique():

            vulnerbaility = readSusVuln(con, i)
            subset_exp = exposuretable[exposuretable[vulnColumn] == i]
            # subset_exp["vuln"]
            subset_exp = pd.merge(left=subset_exp, right=vulnerbaility[[
                                  'vulnAVG', 'mean_x']], how='left', left_on=['class'], right_on=['mean_x'], right_index=False)
            #subset_exp.drop(columns= ['hazIntensity_to'])
            subset_exp = subset_exp.rename(columns={"vulnAVG": "vuln"})
            final_df = pd.DataFrame()
            final_df = final_df.append(subset_exp, ignore_index=True)
        exposuretable = None
        exposuretable = final_df
    else:
        final_df = pd.DataFrame()
        for i in exposuretable[vulnColumn].unique():
            vulnerbaility = readIntVuln(con, i)
            y = vulnerbaility.vulnAVG.values
            x = vulnerbaility.mean_x.values
            subset_exp = exposuretable[exposuretable[vulnColumn] == i]
            subset_exp["vuln"] = np.interp(
                subset_exp.meanHazard, x, y, left=0, right=1)
            final_df = final_df.append(subset_exp, ignore_index=True)
        final_df.loc[final_df.vuln < 0, 'vuln':] = 0
        final_df.loc[final_df.vuln > 1, 'vuln':] = 1

        exposuretable = None
        exposuretable = final_df
    return exposuretable


def calculateLoss(exposuretable, costColumn, spprob=1):
    try:
        exposuretable['loss'] = exposuretable.apply(
            lambda row: row[costColumn]*row.exposed*row.vuln*spprob/100, axis=1)
        losstable = exposuretable.groupby(
            ["geom_id"], as_index=False).agg({'loss': 'sum'})
        losstable_lossonly = losstable[["loss", "geom_id"]]
        return losstable_lossonly
    except Exception as e:
        raise ValueError(f"Caution: Make sure column: '{costColumn}' is valid for loss calculation; Error Detail: {str(e)}")

def calculateLoss_spprob(exposuretable, costColumn, spprob,hazunit):
    
    if hazunit=="classes":  #for classified and binary hazards 
        exposuretable = exposuretable.merge(
            spprob[['sp', 'sp_map_value']], left_on='meanHazard', right_on='sp_map_value', suffixes=('_left', '_right'))
        exposuretable['loss'] = exposuretable.apply(
            lambda row: row[costColumn]*row.exposed*row.vuln*row.sp/100, axis=1)
    else: #for non-classified hazard
        # exposuretable = exposuretable.merge(
        #     spprob[['sp', 'sp_map_value']], left_on='meanHazard', right_on='sp_map_value', suffixes=('_left', '_right'))
        
        for index, row in spprob.iterrows():
            if isinstance(row.val1, str) and row.val1.lower()=="min":
                exposuretable.loc[exposuretable.meanHazard < float(row.val2), 'sp'] = row.sp_val
            elif isinstance(row.val2, str) and row.val2.lower()=="max":
                exposuretable.loc[exposuretable.meanHazard > float(row.val1), 'sp'] = row.sp_val
            else: 
                condition = (exposuretable['meanHazard'] > float(row.val1)) & (exposuretable['meanHazard'] < float(row.val2))
                exposuretable.loc[condition, 'sp'] = row.sp_val
        
        exposuretable['loss'] = exposuretable.apply(
            lambda row: row[costColumn]*row.exposed*row.vuln*row.sp/100, axis=1)
        
    losstable = exposuretable.groupby(
        ["geom_id"], as_index=False).agg({'loss': 'sum'})
    losstable_lossonly = losstable[["loss", "geom_id"]]
    return losstable_lossonly


def ComputeLoss(con, exposureid, lossid, computecol='counts', **kwargs):
    # computeonvalue column has been changed from computation column where 'Cost','Population','Geometry','Count' should be passed.
    is_aggregated = kwargs.get('is_aggregated', False)
    onlyaggregated = kwargs.get('only_aggregated', False)
    adminid = kwargs.get('adminunit_id', None)

    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    base = float(metadata["base"]) or 0
    threshold = float(metadata["threshold"])
    stepsize = float(metadata["stepsize"])
    haztype = metadata["hazintensity"]
    hazunit = metadata["hazunit"]
    vulnColumn = metadata["vulnColumn"]
    schema = metadata["Schema"]
    spprob = metadata["spprob"]
    spprob_single = metadata["spprob_single"]
    
    exposure = getHazardMeanIntensity(exposure, stepsize, base, threshold)
    exposure = estimatevulnerability(exposure, haztype, vulnColumn, con)
    
    if computecol == 'Cost':
        costColumn = metadata["costColumn"]
    elif computecol == 'Population':
        costColumn = metadata["populColumn"]
    elif computecol == 'Geometry':
        costColumn = 'areaOrLen'
    else:
        exposure['counts'] = 1
        costColumn = 'counts'
    # if spprob == None:
    #     loss = calculateLoss(exposure, costColumn)
    if spprob_single:
        loss = calculateLoss(exposure, costColumn)
    else:
        loss = calculateLoss_spprob(exposure, costColumn, spprob,hazunit)

    if is_aggregated:
        admin_unit = readvector.readAdmin(con, adminid)
        admin_unit = gpd.GeoDataFrame(admin_unit, geometry="geom")
        earid = metadata["earID"]
        earpk = metadata['earPK']
        adminmeta = readmeta.getAdminMeta(con, adminid)
        adminpk = adminmeta.col_admin[0] or adminmeta.data_id[0]
        admin_dataid = adminmeta.data_id[0]
        ear = readvector.readear(con, earid)
        loss = pd.merge(left=loss, right=ear[[
                        earpk, 'geom']], left_on='geom_id', right_on=earpk, right_index=False)
        loss = gpd.GeoDataFrame(loss, geometry='geom')

        # check whether adminpk and ear columns have same name, issue #80
        df_columns = list(loss.columns)
        if adminpk in df_columns:
            loss = loss.rename(columns={adminpk: f"{adminpk}_ear"})

        loss = aggregator.aggregateloss(
            loss, admin_unit, adminpk, admin_dataid)
        loss = loss.rename(
            columns={adminpk: 'admin_id', admin_dataid: "geom_id"})
        #merging the data to include all admin unit
        loss= pd.merge(left=loss, right=admin_unit[[adminpk,
                        admin_dataid]], left_on='geom_id', right_on=admin_dataid, right_index=False, how="right")
        loss=loss.drop(columns= ['geom_id','admin_id'])
        loss = loss.rename(
            columns={admin_dataid: "geom_id",adminpk:"admin_id"})
        loss=loss.fillna(0)
        assert not loss.empty, f"The aggregated dataframe in loss returned empty"

    # Non aggrigated case
    else:
        loss['admin_id'] = ''

    # common tasks for both results
    loss['loss_id'] = lossid
    writevector.writeLoss(loss, con, schema)
