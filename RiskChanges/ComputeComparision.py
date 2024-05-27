from .RiskChangesOps.readmeta import getCostBenefitAnalysisMeta
import pandas as pd
import numpy_financial as npf
import json
import numpy as np
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
        
        study_period : int
            Effectiveness period of the applied measures in year (For eg : 50, 20)
            
        discounting_rate (_optional_) : float
            
        investment_collection : JSON
            Example: [
            {"investment_per_year":3502397.33, "start_year":2020, "end_year":""},
            {"investment_per_year":2912405.63, "start_year":2021, "end_year":""},
            {"investment_per_year":2687210.63, "start_year":2022, "end_year":""},
            ]
        maintenance_collection : JSON
            Example: [{"cost_per_year":20857.3, "start_year":2023, "end_year":2050}]
            
        risk_reduction_collection : JSON
            Example 1 (Current Scenario): [{"reduction_per_year":249659, "start_year":4, "end_year":31}]
            Example 2 (Future Scenario): [
                            {"reduction_per_year":0, "start_year":2020, "end_year":""},
                            {"reduction_per_year":0, "start_year":2021, "end_year":""},
                            {"reduction_per_year":0, "start_year":2022, "end_year":""},
                            {"start_year_reduction":315613, "start_year":2023, "end_year":2035, "end_year_reduction": 727695, "interpolation_method":"Linear"},
                            {"start_year_reduction":762035, "start_year":2036, "end_year":2050, "end_year_reduction": 1242797, "interpolation_method":"Linear"},
                        ]
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
        
        
        # for investment in investment_collection:
        #     for i in range(investment['start_year'], investment['start_year']+investment['period']):
        #         if i in df['year'].values:
        #             df.loc[df['year'] == i, 'cost'] += investment['investment_per_year']
        #         else:
        #             data = {"year": i, 'cost': investment['investment_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
        #             df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
        # if maintenance_collection:    
        #     for value in maintenance_collection:
        #         for i in range(value['start_year'], value['start_year']+value['period']):
        #             if i in df['year'].values:
        #                 df.loc[df['year'] == i, 'cost'] += value['cost_per_year']
        #             else:
        #                 data = {"year": i, 'cost': value['cost_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
        #                 df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
                    
        # for value in risk_reduction_collection:
        #     for i in range(value['start_year'], value['start_year']+value['period']):
        #         if i in df['year'].values:
                    
        #             df.loc[df['year'] == i, 'risk_reduction'] += value['reduction_per_year']
        #         else:
        #             data = {"year": i, 'cost': 0, 'risk_reduction': value['reduction_per_year'], 'incremental_benefit': 0}
        #             df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
        
        for investment in investment_collection:
            investment_end_year=investment['end_year'] if investment['end_year'] else investment['start_year']
            for i in range(investment['start_year'], investment_end_year+1):
                if i in df['year'].values:
                    df.loc[df['year'] == i, 'cost'] += investment['investment_per_year']
                else:
                    data = {"year": i, 'cost': investment['investment_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
                    df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
                
        for value in maintenance_collection:
            maintenance_end_year=value['end_year'] if value['end_year'] else value['start_year']
            for i in range(value['start_year'], maintenance_end_year+1):
                if i in df['year'].values:
                    df.loc[df['year'] == i, 'cost'] += value['cost_per_year']
                else:
                    data = {"year": i, 'cost': value['cost_per_year'], 'risk_reduction': 0, 'incremental_benefit': 0}
                    df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
                    
        for value in risk_reduction_collection:
            if "interpolation_method" in value.keys():
                start_value=value['start_year_reduction']
                end_value=value['end_year_reduction']
                intermediate_values_no=value['end_year']-value['start_year']-1
                intermediate_values = np.linspace(start_value, end_value, intermediate_values_no+2)[1:-1]
                intermediate_values=[start_value]+intermediate_values.tolist()+[end_value]

                for index,i in enumerate(range(value['start_year'], value['end_year']+1)):
                    if i in df['year'].values:
                        df.loc[df['year'] == i, 'risk_reduction'] = intermediate_values[index]
                    else:
                        data = {"year": i, 'cost': 0, 'risk_reduction': intermediate_values[index], 'incremental_benefit': 0}
                        df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
            else:
                risk_reduction_end_year=value['end_year'] if value['end_year'] else value['start_year']
                for i in range(value['start_year'], risk_reduction_end_year+1):
                    if i in df['year'].values:
                        df.loc[df['year'] == i, 'risk_reduction'] = value['reduction_per_year']
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
        return True, "success",cb_ratio, np_npv, np_irr*100
    
    except Exception as e:
        return False, str(e), None, None, None
