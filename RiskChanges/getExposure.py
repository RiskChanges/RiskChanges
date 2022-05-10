import geopandas as gpd
import pandas as pd
import numpy as np

from .RiskChangesOps.readvulnerability import readIntVuln, readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator

def getSummary(con,exposureid,agg=False):
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid=metadata.hazid
    classificationScheme=readmeta.classificationscheme(con,hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col=metadata["TypeColumn"]
    stepsize = float(metadata["stepsize"])
    min_thresholds=np.arange(start=base,stop=maxval+stepsize,step=stepsize).tolist()
    convert_dict={}
    for i in min_thresholds:
        name=classificationScheme.query(f'val1 == {i}')['class_name'].to_list()[0]
        convert_dict[str(i)]=name
    exposure = exposure.astype({"class": str})
    exposure["class"].replace(convert_dict, inplace=True)
    if not agg:
        summary=pd.pivot_table(exposure, values='areaOrLen', index=[type_col],
                        columns=["class"], aggfunc=np.sum, fill_value=0)
    else:
        summary=pd.pivot_table(exposure, values='exposed_areaOrLen', index=[type_col],
                        columns=["class"], aggfunc=np.sum, fill_value=0)        
    return summary


def getShapefile(con,exposureid):
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid=metadata.hazid
    classificationScheme=readmeta.classificationscheme(con,hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col=metadata["TypeColumn"]
    stepsize = float(metadata["stepsize"])
    min_thresholds=np.arange(start=base,stop=maxval+stepsize,step=stepsize).tolist()
    convert_dict={}
    for i in min_thresholds:
        name=classificationScheme.query(f'val1 == {i}')['class_name'].to_list()[0]
        convert_dict[str(i)]=name
    exposure = exposure.astype({"class": str})
    exposure["class"].replace(convert_dict, inplace=True)
    