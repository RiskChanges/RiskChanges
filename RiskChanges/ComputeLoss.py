import geopandas as gpd
import pandas as pd
import numpy as np

from .RiskChangesOps.readvulnerability import readIntVuln,readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator



def getHazardMeanIntensity(exposuretable,stepsize,base):
    stepsize=stepsize#5 #import from database
    base=base#0 #import from database
    half_step=stepsize/2
    exposuretable['meanHazard']=base+exposuretable['class']*stepsize-half_step
    return exposuretable
def estimatevulnerability(exposuretable,haztype,vulnColumn,con):
        if haztype=="Susceptibility":
            for i in exposuretable[vulnColumn].unique():
                
                vulnerbaility=readSusVuln(con,i)
                subset_exp=exposuretable[exposuretable[vulnColumn]==i]
                #subset_exp["vuln"]
                subset_exp=pd.merge(left=subset_exp, right=vulnerbaility[['vulnAVG', 'mean_x']], how='left', left_on=['class'], right_on=['mean_x'],right_index=False)
                #subset_exp.drop(columns= ['hazIntensity_to'])
                subset_exp.rename(columns={"vulnAVG": "vuln"})
                final_df=final_df.append(subset_exp, ignore_index = True)   
            exposuretable=None
            exposuretable=final_df
        else:  
            final_df=pd.DataFrame()
            for i in exposuretable[vulnColumn].unique(): 
                vulnerbaility=readIntVuln(con,i)
                y=vulnerbaility.vulnAVG.values 
                x=vulnerbaility.mean_x.values 
                subset_exp=exposuretable[exposuretable[vulnColumn]==i]
                subset_exp["vuln"]=np.interp(subset_exp.meanHazard, x, y, left=0, right=1)
                final_df=final_df.append(subset_exp, ignore_index = True)
            final_df.loc[final_df.vuln<0,'vuln':]=0
            final_df.loc[final_df.vuln>1,'vuln':]=1
                
            exposuretable=None
            exposuretable=final_df
        return exposuretable
def calculateLoss(exposuretable,costColumn,spprob=1):
    exposuretable['loss'] = exposuretable.apply(lambda row: row[costColumn]*row.exposed*row.vuln*spprob/100, axis=1)
    losstable=exposuretable.groupby(["geom_id"],as_index=False).agg({'loss':'sum'})
    losstable_lossonly=losstable[["loss", "geom_id"]]
    return losstable_lossonly

def calculateLoss_spprob(exposuretable,costColumn,spprob):
    exposuretable=exposuretable.merge(spprob[['sp','sp_map_value']], left_on='meanHazard', right_on='sp_map_value',suffixes=('_left', '_right'))
    exposuretable['loss'] = exposuretable.apply(lambda row: row[costColumn]*row.exposed*row.vuln*row.sp/100, axis=1)
    losstable=exposuretable.groupby(["geom_id"],as_index=False).agg({'loss':'sum'})
    losstable_lossonly=losstable[["loss", "geom_id"]]
    return losstable_lossonly

def ComputeLoss(con,exposureid,lossid,computeonvalue=True,**kwargs):
    is_aggregated = kwargs.get('is_aggregated', False)
    onlyaggregated = kwargs.get('only_aggregated', False)
    adminid = kwargs.get('adminunit_id', None)

    metadata=readmeta.computeloss_meta(con,exposureid)
    exposure=readvector.prepareExposureForLoss(con,exposureid)
    base=float(metadata["base"])
    stepsize=float(metadata["stepsize"])
    haztype=metadata["hazintensity"]
    vulnColumn=metadata["vulnColumn"]
    schema=metadata["Schema"]
    spprob=metadata["spprob"]
    spprob_single=metadata["spprob_single"]
    exposure=getHazardMeanIntensity(exposure,stepsize,base)
    exposure=estimatevulnerability(exposure,haztype,vulnColumn,con)
    if computeonvalue:
        costColumn=metadata["costColumn"]
    else:
        costColumn=metadata["populColumn"]
    if spprob==None:
        loss=calculateLoss(exposure,costColumn)
    elif spprob_single:
        loss=calculateLoss(exposure,costColumn,spprob)
    else:
        loss=calculateLoss(exposure,costColumn,spprob)
    loss['loss_id']=lossid
    if not onlyaggregated:
        writevector.writeLoss(loss,con,schema)

    if is_aggregated:
        admin_unit=readvector.readAdmin(con,adminid)
        earid=metadata["earID"]
        ear= readvector.readear(con,earid)
        loss=pd.merge(left=loss, right=ear['id','geom'], left_on='geom_id',right_on='id',right_index=False)
        loss= gpd.GeoDataFrame(loss,geometry='geom')
        loss=aggregator.aggregateloss(loss,admin_unit)
        writevector.writeLossAgg(df,con,schema)



    



