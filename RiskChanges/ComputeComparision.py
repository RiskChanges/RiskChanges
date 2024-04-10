from .RiskChangesOps.readmeta import getCostBenefitAnalysisMeta
import pandas as pd
import numpy_financial as npf
import json
# import logging
# logger = logging.getLogger(__file__)

def CalculateCostBenefit(**kwargs):
# def CalculateCostBenefit(con, cost_benefit_id, **kwargs):
    """
    if cost_benefit_id is None, all required kwards must be provided
    
    Args:
        con (_str_): _To connect database_
        cost_benefit_id (_int_): _To exteact metadata of given id_
        
    Kwargs:
        # current_risk_id : int
        #     RiskIndex Model id with Alternative A0
            
        # future_risk_id : int
        #     RiskIndex Model id with Alternative A1 or A2 or A33
        
        
        # investment_period : int
        #     Time for construction
        #     Investment period of the risk reduction measure in year (For eg : 5, 2)
        
        # investment_per_year : float
        #     Investment per year to implement measure
        
        # benefits_start_year : int
        #     In which year after the start of the construction are the benefits achieved
        #     For eg : 5, 2
        
        # benefits_per_year : float
        #     This is calculated by substracting AAL(A0) from AAL(A1)
        
        # maintenance_start_year (_optional_) : int
        #     In which year after the start of the construction are the maintenance required
        
        # maintenance_cost_per_year (_optional_) : float
        #     Maintenance cost per year
        
        study_period : int
            Effectiveness period of the applied measures in year (For eg : 50, 20)
            
        discounting_rate (_optional_) : float
            
        investment_collection : dict
            Example: [
                        {"name":"Storage basin construction",'investment_per_year':2379256.67, 'start_year':1, 'period':1},
                        {"name":"Slope Stabilization",'investment_per_year':1775816.67, 'start_year':2, 'period':2}
                    ]
        maintenance_collection : dict
            Example: [{'cost_per_year':30000, 'start_year':4, 'period':28}]
            
        risk_reduction_collection : dict
            Example: [{'reduction_per_year':315613, 'start_year':4, 'period':28}]
            
    Output:  
        npv : float
            Net present value
            
        irr : float
            Internal rate of return
            
        cb_ratio : float
            cost benefit ratio

    """
    cb_ratio=None
    np_npv=None
    np_irr=None
    try:
        discounting_rate = kwargs.get('discounting_rate', None)
        investment_collection = kwargs.get('investment_collection', None)
        maintenance_collection = kwargs.get('maintenance_collection', None)
        risk_reduction_collection = kwargs.get('risk_reduction_collection', None)
        
        if isinstance(investment_collection, str):
            investment_collection=json.loads(investment_collection)
            
        if maintenance_collection and isinstance(maintenance_collection, str):
            maintenance_collection=json.loads(maintenance_collection)
            
        if isinstance(risk_reduction_collection, str):
            risk_reduction_collection=json.loads(risk_reduction_collection)
            
        if discounting_rate and isinstance(discounting_rate, str):
            discounting_rate=float(discounting_rate)
        
        df = pd.DataFrame(columns=['year', 'cost', 'risk_reduction','incremental_benefit']) 
        for investment in investment_collection:
            for i in range(investment['start_year'], investment['start_year']+investment['period']):
                if i in df['year'].values:
                    df.loc[df['year'] == i, 'cost'] += investment['investment_per_year']
                else:
                    data = {"year": i, 'cost': investment['investment_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
                    df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
        if maintenance_collection:    
            for value in maintenance_collection:
                for i in range(value['start_year'], value['start_year']+value['period']):
                    if i in df['year'].values:
                        df.loc[df['year'] == i, 'cost'] += value['cost_per_year']
                    else:
                        data = {"year": i, 'cost': value['cost_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
                        df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
                    
        for value in risk_reduction_collection:
            for i in range(value['start_year'], value['start_year']+value['period']):
                if i in df['year'].values:
                    
                    df.loc[df['year'] == i, 'risk_reduction'] += value['reduction_per_year']
                else:
                    data = {"year": i, 'cost': 0, 'risk_reduction': value['reduction_per_year'], 'incremental_benefit': 0}
                    df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
                    
        df['incremental_benefit']=df['risk_reduction']-df['cost']
        incremental_benefit_list=[0]+df['incremental_benefit'].tolist()
        
        if discounting_rate:
            discount_rate=discounting_rate/100
            np_npv=npf.npv(discount_rate,incremental_benefit_list)
        np_irr=npf.irr(incremental_benefit_list)
        cb_ratio=sum(df['cost'])/sum(df['incremental_benefit'])
        # # print(df)
        return True, "success",cb_ratio, np_npv, np_irr*100
    
    except Exception as e:
        return False, str(e), None, None, None
