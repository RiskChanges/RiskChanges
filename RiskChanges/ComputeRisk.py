import random
import string
from sklearn.metrics import auc
import geopandas as gpd
import pandas as pd
import numpy as np
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator

def find_duplicates(arr):
    rounded_array_list=[round(num, 6) for num in arr]
    duplicates = []
    indices = {}
    for i, value in enumerate(rounded_array_list):
        if value in indices:
            if value not in duplicates:
                duplicates.append(value)
            indices[value].append(i)
        else:
            indices[value] = [i]
    return duplicates, indices


def dutch_method(xx, yy):
    # compute risk based on dutch method where xx is value axis and yy is probability axis
    # duplicates, indices = find_duplicates(xx)
    # if len(duplicates)>0:
    #     for key in indices.keys():
    #         if key!=0 and len(indices[key])>1:
    #             increment=0.0001
    #             for i in indices[key]:
    #                 xx[i]=xx[i]+increment if xx[i]+increment not in xx else xx[i]
    #                 increment=increment+0.0001
    # args = np.argsort(np.array(xx,dtype=np.float64))
    
    args = np.argsort(np.array(yy,dtype=np.float64))[::-1]
    xx = [xx[i] for i in args]
    yy = [yy[i] for i in args]
    AAL = auc(x=xx, y=yy)+(xx[0]*yy[0])
    return AAL


def checkUniqueHazard(connstr, lossids):
    blank_list = []
    for lossid in lossids:
        haztype = readmeta.getHazardType(connstr, lossid)
        blank_list.append(haztype)
    assert pd.Series(blank_list).nunique(
    ) == 1, "Only multiple return periods of single hazard is supported"


def id_generator(size=4, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def PrepareLossForRisk(con, lossids):
    i = True
    cols = []
    probs = []
    for id in lossids:
        lossdata = readvector.readLoss(con, id)
        return_period = float(readmeta.getReturnPeriod(con, id))
        colname = 'loss_rp_'+id_generator() + "_" + str(return_period)
        cols.append(colname)
        probs.append(1.0/return_period)
        lossdata = lossdata.rename(columns={'loss': colname})
        if i:
            prepared_loss = lossdata
            i = False
        else:
            prepared_loss = prepared_loss.merge(lossdata, on='geom_id',how='outer')
    prepared_loss[cols]=prepared_loss[cols].fillna(value=0)
    return prepared_loss, cols, probs


def calculateRisk(lossdf, columns, probs):
    risktable = pd.DataFrame(columns=['geom_id', 'AAL'])
    for index, row in lossdf.iterrows():
        xx = row[columns].values.tolist()
        yy = probs
        aal = dutch_method(xx, yy)
        ear_id = row['geom_id']
        new_row = {'geom_id': ear_id, 'AAL': aal}
        print("********************")
        print(xx,"xx")
        print(yy,"yy")
        print(aal,"aal")
        print(ear_id,"ear_id")
        # append row to the dataframe
        risktable = risktable.append(new_row, ignore_index=True)
    assert not risktable.empty, f"The Risk calculation failed"
    return risktable


def ComputeRisk(con, lossids, riskid, **kwargs):
    is_aggregated = kwargs.get('is_aggregated', False)
    onlyaggregated = kwargs.get('only_aggregated', False)
    adminid = kwargs.get('adminunit_id', None)

    checkUniqueHazard(con, lossids)
    lossdf, columns, probs = PrepareLossForRisk(con, lossids)

    risk = calculateRisk(lossdf, columns, probs)
    metatable = readmeta.readLossMeta(con, lossids[0])
    schema = metatable.workspace[0]

    if is_aggregated:
        admin_unit = readvector.readAdmin(con, adminid)
        admin_unit = gpd.GeoDataFrame(admin_unit, geometry="geom")
        ear_id = metatable['ear_index_id'][0]
        earmeta = readmeta.earmeta(con, ear_id)
        earPK = earmeta.data_id[0]
        ear = readvector.readear(con, ear_id)
        adminmeta = readmeta.getAdminMeta(con, adminid)
        admin_dataid = adminmeta.data_id[0]
        adminpk = adminmeta.col_admin[0] or adminmeta.data_id[0]
        risk = pd.merge(left=risk, right=ear[[earPK, 'geom']],
                        left_on='geom_id', right_on=earPK, right_index=False)
        risk = gpd.GeoDataFrame(risk, geometry='geom')

        # check whether adminpk and ear columns have same name, issue #80
        df_columns = list(risk.columns)
        if adminpk in df_columns:
            risk = risk.rename(columns={adminpk: f"{adminpk}_ear"})

        # aggrigate result
        risk = aggregator.aggregaterisk(
            risk, admin_unit, adminpk, admin_dataid)
        risk = risk.rename(
            columns={adminpk: 'admin_id', admin_dataid: "geom_id"})

        #merging the data to include all admin unit
        risk= pd.merge(left=risk, right=admin_unit[[adminpk,
                        admin_dataid]], left_on='geom_id', right_on=admin_dataid, right_index=False, how="right")
        risk=risk.drop(columns= ['geom_id','admin_id'])
        risk = risk.rename(
            columns={admin_dataid: "geom_id",adminpk:"admin_id"})
        risk=risk.fillna(0)
        assert not risk.empty, f"The aggregated dataframe in risk returned empty"

    # Non aggrigate case
    else:
        risk['admin_id'] = ''

    # common tasks for both results
    risk['risk_id'] = riskid
    writevector.writeRisk(risk, con, schema)
