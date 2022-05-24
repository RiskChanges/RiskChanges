import geopandas as gpd
import pandas as pd
import numpy as np

from .RiskChangesOps.readvulnerability import readIntVuln, readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator


def getSummary(con, exposureid, agg=False):
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata['hazid']
    classificationScheme = readmeta.classificationscheme(con, hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(start=base, stop=maxval, step=stepsize).tolist()
    convert_dict = {}
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure['class'].replace(convert_dict, inplace=True)

    if not agg:
        summary = pd.pivot_table(exposure, values='areaOrLen', index=[type_col],
                                 columns=["class"], aggfunc=np.sum, fill_value=0)
    else:
        summary = pd.pivot_table(exposure, values='exposed_areaOrLen', index=[type_col],
                                 columns=["class"], aggfunc=np.sum, fill_value=0)
    return summary


def getShapefile(con, exposureid, agg=False):
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata["hazid"]
    classificationScheme = readmeta.classificationscheme(con, hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    earpk = metadata["earPK"]
    earid = metadata['earID']
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(
        start=base, stop=maxval, step=stepsize).tolist()
    convert_dict = {}
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure["class"].replace(convert_dict, inplace=True)
    if not agg:
        summary=pd.pivot_table(exposure, values='exposed', index=['geom_id'],
                        columns=["class"], aggfunc=np.sum, fill_value=0)
        ear = readvector.readear(con, earid)
        summary = pd.merge(left=summary, right=ear[[
                        earpk, 'geom']], left_on='geom_id', right_on=earpk, right_index=False)
        summary = gpd.GeoDataFrame(summary, geometry='geom')                    
    else:
        print('hello')
        #aggegate based on the column in EAR which might be value, nr people or area.. TODO: change function in aggregation of EAR then use same approach here. 
        #also depending on the classess 
    return summary

    '''  Change the exposure function to add admin unit name in exposure result if aggregation is true and make OTF funtion to retrive those information.. '''
        summary = pd.pivot_table(exposure, values='exposed', index=['geom_id'],
                                 columns=["class"], aggfunc=np.sum, fill_value=0)
    ear = readvector.readear(con, earid)
    summary = pd.merge(left=summary, right=ear[[
        earpk, 'geom']], left_on='geom_id', right_on=earpk, right_index=False)
    summary = gpd.GeoDataFrame(summary, geometry='geom')
    return summary
