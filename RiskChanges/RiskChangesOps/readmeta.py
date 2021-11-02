import pandas as pd
import psycopg2
def earmeta(connstr,earid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_earindex" as eartable WHERE eartable.id={earid};'
    print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The EAR id {earid} do not exists"
    return metatable

def hazmeta(connstr,hazid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_hazardindex" as haztable WHERE haztable.id={hazid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The hazard id {hazid} do not exists"
    return metatable

def exposuremeta(connstr,exposureid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."exposure_exposureindex" as exposuretable WHERE exposuretable.id={exposureid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The exposure id {exposureid} do not exists"
    return metatable

def loadspprob(connstr,hazardid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_spclass" as spprob WHERE spprob.hazard_id={hazardid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The hazard id for spatial probability {hazardid} do not exists"
    return metatable

def computeloss_meta(connstr,exposureid):
    #get it from exposure
    meta=exposuremeta(connstr,exposureid)
    exposureTable=meta.exposure_table[0]
    earID=meta.ear_index_id[0]
    hazid=meta.hazard_index_id[0]
    # earPK=meta.ear_index_primary_key[0]
    #get it from EAR
    metaear=earmeta(connstr,earID)
    Schema=metaear.workspace[0]
    earPK=metaear.data_id[0]
    # #ask tek?
    costColumn=metaear.col_value_avg[0]
    populColumn=metaear.col_population_avg[0]
    TypeColumn=metaear.col_class[0]
    #get it from hazard
    metahaz=hazmeta(connstr,hazid)
    hazunit=metahaz.unit[0]
    hazintensity=metahaz.intensity[0]
    base=metahaz.base_val[0]
    stepsize=metahaz.interval_val[0]
    vulnColumn=metahaz.type[0]
    spprob=metahaz.sp_val[0]
    spprob_single=True
    if spprob==0:
        spprob=loadspprob(connstr,hazid)
        #spprob=spprob.set_index('sp_map_value')['sp'].to_dict()
        spprob_single=False
    lossmeta={'spprob':spprob,'spprob_single':spprob_single,'exposureTable':exposureTable,'earID':earID,'hazid':hazid,'earPK':earPK,'Schema':Schema,'costColumn':costColumn,'populColumn':populColumn,'TypeColumn':TypeColumn,'hazunit':hazunit,'hazintensity':hazintensity,'base':base,'stepsize':stepsize,'vulnColumn':vulnColumn}
    return lossmeta

def readLossMeta(connstr,lossid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."loss_lossindex" as lossindex WHERE lossindex.id={lossid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The loss id {lossid} do not exists"
    return metatable

def getReturnPeriod(connstr,lossid):
    lossmeta=readLossMeta(connstr,lossid)
    hazindex=lossmeta.hazard_index_id[0]
    metahaz=hazmeta(connstr,hazindex)
    return_period=metahaz.rp_avg[0]
    return return_period

def getHazardType(connstr,lossid):
    lossmeta=readLossMeta(connstr,lossid)
    hazindex=lossmeta.hazard_index_id[0]
    metahaz=hazmeta(connstr,hazindex)
    hazardType=metahaz.type[0]
    return hazardType

def getRiskMeta(connstr,riskid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."risk_riskindex" as riskindex WHERE riskindex.id={riskid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The risk id {riskid} do not exists"
    return metatable
def getAdminMeta(connstr,adminid):
    #SELECT * FROM public."projectIndex_adminunit"
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_adminunit" as adminindex WHERE adminindex.id={adminid};'
    #print(sql_val)
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The admin unit id {adminid} do not exists"
    return metatable