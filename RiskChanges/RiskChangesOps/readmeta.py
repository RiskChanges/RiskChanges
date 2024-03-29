import pandas as pd
import psycopg2

def earmeta(connstr,earid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_earindex" as eartable WHERE eartable.id={earid};'
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The EAR id {earid} do not exists"
    return metatable

def classificationscheme(connstr,hazid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_intensityclass" as classtable WHERE classtable.hazard_id={hazid};'
    classtable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not classtable.empty , f"The classification scheme for hazard id  {hazid} do not exists"
    return classtable

def hazmeta(connstr,hazid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_hazardindex" as haztable WHERE haztable.id={hazid};'
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
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The exposure id {exposureid} do not exists"
    return metatable

def loadspprob(connstr,hazardid,hazunit):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    if hazunit=="classes":
        sql_val=f'SELECT * FROM public."projectIndex_spclass" as spprob WHERE spprob.hazard_id={hazardid};'
    else:
        sql_val=f'SELECT * FROM public."projectIndex_intensityclass" as spprob WHERE spprob.hazard_id={hazardid};'
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The hazard id for spatial probability {hazardid} do not exists"
    return metatable

def computeloss_meta(connstr,exposureid):
    #get it from exposure
    meta=exposuremeta(connstr,exposureid)
    adminid=meta.admin_unit_id[0]
    exposureTable=meta.exposure_table[0]
    earID=meta.ear_index_id[0]
    hazid=meta.hazard_index_id[0]
    
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
    threshold=metahaz.threshold_val[0]
    stepsize=metahaz.interval_val[0]
    vulnColumn=metahaz.type[0]
    
    # spprob=metahaz.sp_val[0]
    # spprob_single=True
    # if spprob==0:
    #     spprob=loadspprob(connstr,hazid)
    #     #spprob=spprob.set_index('sp_map_value')['sp'].to_dict()
    #     spprob_single=False
    # lossmeta={'spprob':spprob,'spprob_single':spprob_single,'exposureTable':exposureTable,'earID':earID,'hazid':hazid,'earPK':earPK,'Schema':Schema,'costColumn':costColumn,'populColumn':populColumn,'TypeColumn':TypeColumn,'hazunit':hazunit,'hazintensity':hazintensity,'base':base,'stepsize':stepsize,'vulnColumn':vulnColumn,'threshold':threshold,'adminid':adminid}
    
    spprob_single=True
    spprob=None
    if hazintensity=="Susceptibility":
        spprob=loadspprob(connstr,hazid,hazunit)
        spprob_single=False
    lossmeta={'spprob':spprob,'spprob_single':spprob_single,'exposureTable':exposureTable,'earID':earID,'hazid':hazid,'earPK':earPK,'Schema':Schema,'costColumn':costColumn,'populColumn':populColumn,'TypeColumn':TypeColumn,'hazunit':hazunit,'hazintensity':hazintensity,'base':base,'stepsize':stepsize,'vulnColumn':vulnColumn,'threshold':threshold,'adminid':adminid}
    
    return lossmeta

def readLossMeta(connstr,lossid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."loss_lossindex" as lossindex WHERE lossindex.id={lossid};'
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
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The risk id {riskid} do not exists"
    return metatable

def getAdminMeta(connstr,adminid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_adminunit" as adminindex WHERE adminindex.id={adminid};'
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The admin unit id {adminid} do not exists"
    return metatable

def getHazardIntensityClasses(connstr,hazard_id):
    '''
    Using this function in loss calculation output save in database
    '''
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_intensityclass" as intensityclass WHERE intensityclass.hazard_id={hazard_id};'
    hazard_intensity_classes=pd.read_sql(sql_val,engine)
    engine.close()
    assert not hazard_intensity_classes.empty , f"The IntensityClasses with hazard id {hazard_id} do not exists"
    return hazard_intensity_classes

def getEarClassAlias(connstr,ear_id):
    '''
    Using this function in loss calculation output save in database
    '''
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_earclassalias" as earclassalias WHERE earclassalias.ear_id={ear_id};'
    ear_class_alias=pd.read_sql(sql_val,engine)
    engine.close()
    assert not ear_class_alias.empty , f"The EarClassAlias with ear id {ear_id} do not exists"
    return ear_class_alias

def getRasterEarDiscreteClass(connstr,ear_id):
    '''
    Using this function in loss calculation output save in database
    '''
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_rastereardiscreteclass" as discreteclass WHERE discreteclass.ear_id={ear_id};'
    discrete_class=pd.read_sql(sql_val,engine)
    engine.close()
    assert not discrete_class.empty , f"The EarClassAlias with ear id {ear_id} do not exists"
    return discrete_class

def getRasterEarMeta(connstr,earid):
    try: 
        engine=psycopg2.connect(connstr)
    except :
        print("unable to create connection")
    sql_val=f'SELECT * FROM public."projectIndex_rasterearindexmetadata" as metadata WHERE metadata.ear_id={earid};'
    metatable=pd.read_sql(sql_val,engine)
    engine.close()
    assert not metatable.empty , f"The EAR id {earid} do not exists"
    return metatable