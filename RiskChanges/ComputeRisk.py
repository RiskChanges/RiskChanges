import RiskChangesOps.readmeta as readmeta
import RiskChangesOps.readvector as readvector
from sklearn.metrics import auc
import RiskChangesOps.writevector as writevector
import RiskChangesOps.AggregateData as aggregator
import geopandas as gpd
import pandas as pd
def dutch_method(xx,yy):
        #compute risk based on dutch method where xx is value axis and yy is probability axis
        AAL=auc(yy,xx)+(xx[0]*yy[0])
        return AAL

def checkUniqueHazard(con, lossids):
    blank_list=[]
    for lossid in lossids:
        haztype=readmeta.getHazardType(connstr,lossid)
        blank_list.append(haztype)
    assert pd.Series(blank_list).nunique()==1, "Only multiple return periods of single hazard is supported"

def PrepareLossForRisk(con, lossids):
    i=True
    cols=[]
    probs=[]
    for id in lossids:
        lossdata=readvector.readLoss(con,id)
        return_period=float(readmeta.getReturnPeriod(con,id))
        colname='loss_rp_'+str(return_period)
        cols.append(colname)
        probs.append(1.0/return_period)
        lossdata=lossdata.rename(columns={'loss': colname,'geom_id':'Unit_ID'})
        if i:
            prepared_loss=lossdata
            i=False
        else:
            prepared_loss=prepared_loss.merge(lossdata, on='Unit_ID')
    return prepared_loss,cols,probs

def calculateRisk(lossdf,columns, probs):
    risktable=pd.DataFrame(columns=['Unit_ID','AAL'])
    for index, row in lossdf.iterrows():
        xx=row[columns].values.tolist()
        yy=probs
        aal=dutch_method(xx,yy)
        #print('ear',aal)
        ear_id=row['Unit_ID']
        new_row = {'Unit_ID':ear_id, 'AAL':aal}
        #append row to the dataframe
        risktable =risktable.append(new_row, ignore_index=True)
    assert not risktable.empty , f"The Risk calculation failed"
    return risktable


def ComputeRisk(con,lossids,riskid,**kwargs):
    try:
        is_aggregated=kwargs['is_aggregated']
        onlyaggregated=kwargs['only_aggregated']
        adminid=kwargs['adminunit_id']
    except:
        is_aggregated= False
        onlyaggregated= False
    checkUniqueHazard(con,lossids)
    lossdf,columns, probs=PrepareLossForRisk(con, lossids)
    risk=calculateRisk(lossdf,columns, probs)
    metatable=readmeta.readLossMeta(connstr,lossids[0])
    schema= metatable.workspace[0]
    risk['risk_id']=riskid

    if not onlyaggregated:
        writevector.writeRisk(risk,con,schema)
    if is_aggregated:
        admin_unit=readvector.readAdmin(con,adminid)
        ear_id=metatable['ear_index_id'][0]
        earmeta=readmeta.exposuremeta(con,ear_id)
        earPK=earmeta.data_id[0]
        ear= readvector.readear(con,ear_id)
        adminmeta=readmeta.getAdminMeta(con,adminid)
        adminpk=adminmeta.data_id[0]
        risk=pd.merge(left=risk, right=ear[earPK,'geom'], left_on='Unit_ID',right_on=earPK,right_index=False)
        risk= gpd.GeoDataFrame(risk,geometry='geom')
        risk=aggregator.aggregaterisk(risk,admin_unit,adminpk)
        assert not risk.empty , f"The aggregated dataframe in risk returned empty"
        writevector.writeRiskAgg(risk,con,schema)

 
    