import pandas as pd
import geopandas as gpd
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator

def combineRisks(connstr,riskids,adminunitid,combinedriskid):
    admin_unit=readvector.readAdmin(connstr,adminunitid)
    first=True
    cols=[]
    for riskid in riskids:
        risk_value=readvector.readRiskGeometry(connstr,riskid)
        aggregated_risk=aggregator.aggregateRisk(risk_value,admin_unit)
        col='AAL'+str(riskid)
        cols.append(col)
        if first:
            combinedrisk=aggregated_risk
            combinedrisk=combinedrisk.rename(columns={'AAL':col})
        else:
            combinedrisk=pd.merge(left=combinedrisk, right=aggregated_risk[['Unit_ID','AAL']], how='outer', left_on=['Unit_ID'], right_on=['Unit_ID'],right_index=False).rename(columns={'AAL': col})
    combinedrisk['AAL']=combinedrisk[cols].sum(axis=1)
    combinedrisk=combinedrisk.drop(columns=[cols])
    meta=readmeta.getRiskMeta(connstr,riskids[0])
    schema= meta.workspace[0]
    combinedrisk['risk_id']=combinedriskid
    writevector.writeRiskCombined(combinedrisk,con,schema)
