import geopandas as gpd
import pandas as pd
import numpy as np
from .RiskChangesOps.readvulnerability import readIntVuln, readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator

# import logging
# logger = logging.getLogger(__file__)
#! create function for similar calculation

def add_hazard_class(exposure,min_thresholds,classificationScheme,haz_class_field="class"):
    # Change the classes to the user defined class
    try:
        convert_dict = {}
        for i, val in enumerate(min_thresholds):
            classificationScheme['val1'] = classificationScheme['val1'].astype(
                float) # the default type for val1 is char, change it to float and compare
            name = classificationScheme.query(f'val1 == {val}')
            # not every hazard class are avialable on exposure table, so try except to pass even the class is not available
            try:
                name = name['class_name'].to_list()[0]
                convert_dict[i+1] = name
            except Exception as e:
                return(False,str(e),None)
            
            if (val == min_thresholds[-1]):
                exposure[haz_class_field] = np.where(
                    exposure[haz_class_field] >= i+1, i+1, exposure[haz_class_field])
                
        exposure[haz_class_field].replace(convert_dict, inplace=True)
        return True,exposure,convert_dict
    except Exception as e:
        return False,str(e),None    

def getSummary(con, exposureid, column='areaOrLen', agg=False):
    try:
        if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
            raise ValueError(
                "column: status must be one of areaOrLen, value_exposure or population_exposure")
        metadata = readmeta.computeloss_meta(con, exposureid)
        exposure = readvector.prepareExposureForLoss(con, exposureid)
        hazid = metadata['hazid']
        classificationScheme = readmeta.classificationscheme(con, hazid)
        thresholds=classificationScheme['val1'].unique()
        thresholds.sort()
        min_thresholds=[float(val) for val in thresholds]
        type_col = metadata["TypeColumn"]
        response,add_hazard_class_result,hazard_class_dict=add_hazard_class(exposure,min_thresholds,classificationScheme)
        if response:
            exposure=add_hazard_class_result
        else:
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result}")
            
        # exposure['class'].replace(convert_dict, inplace=True)
        
        # if column is count just count the number of feature exposed
        # if column == 'count':
        #     exposure[column] = 1
            
        if not agg:
            summary = pd.pivot_table(exposure, values=column, index=[type_col],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            summary = summary.reset_index()
            summary = summary.rename(columns={type_col: "Ear Class"})
            
        else:
            summary = pd.pivot_table(exposure, values=column, index=[type_col, 'admin_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            summary = summary.reset_index()
            summary = summary.rename(
                columns={type_col: "Ear Class", "admin_id": "Admin Name"})
        summary = summary.fillna(0)
        #to drop column 0.0 if exists
        if 0.0 in summary.columns:
            summary = summary.drop(0.0, axis=1)
        #Add missing hazard class in table
        for values in hazard_class_dict.values():
            if values not in summary.columns:
                summary[values]=0
                
        other_columns = [col for col in summary.columns if col not in hazard_class_dict.values()]
        desired_columns_order=other_columns+list(hazard_class_dict.values())
        # Step 5: Reorder the columns using reindex with the desired_order followed by the remaining columns
        summary = summary.reindex(columns=desired_columns_order)
        
        return summary
    except Exception as e:
        raise Exception(f"Error in getSummary {str(e)}")


def getShapefile(con, exposureid, column='areaOrLen', agg=False):
    try:
        if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
            raise ValueError(
                "column: status must be one of areaOrLen, value_exposure or population_exposure")
        metadata = readmeta.computeloss_meta(con, exposureid)
        exposure = readvector.prepareExposureForLoss(con, exposureid)
        hazid = metadata["hazid"]
        classificationScheme = readmeta.classificationscheme(con, hazid)
        thresholds=classificationScheme['val1'].unique()
        thresholds.sort()
        min_thresholds=[float(val) for val in thresholds]
        type_col = metadata["TypeColumn"]
        adminid = metadata['adminid']
        earpk = metadata["earPK"]
        earid = metadata['earID']
        response,add_hazard_class_result,hazard_class_dict=add_hazard_class(exposure,min_thresholds,classificationScheme)
        if response:
            exposure=add_hazard_class_result
        else:
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result}")
        
        # if column is count just count the number of feature exposed
        # if column == 'count':
        #     exposure[column] = 1
        if not agg:
            summary = pd.pivot_table(exposure, values=column, index=['geom_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            ear = readvector.readear(con, earid)
            summary = pd.merge(left=summary, right=ear,
                            left_on='geom_id', right_on=earpk, right_index=False)
            summary = gpd.GeoDataFrame(summary, geometry='geom')
        else:
            summary = pd.pivot_table(exposure, values=column, index=['admin_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            admin = readvector.readAdmin(con, adminid)
            adminmeta = readmeta.getAdminMeta(con, adminid)
            adminpk = adminmeta.col_admin[0]
            summary = pd.merge(left=summary, right=admin,
                            left_on='admin_id', right_on=adminpk, right_index=False,how='right')
            summary = gpd.GeoDataFrame(summary, geometry='geom')
        summary = summary.fillna(0)
        #to drop column 0.0 if exists
        if 0.0 in summary.columns:
            summary = summary.drop(0.0, axis=1)
        #Add missing hazard class in table
        for values in hazard_class_dict.values():
            if values not in summary.columns:
                summary[values]=0
        other_columns = [col for col in summary.columns if col not in hazard_class_dict.values()]
        desired_columns_order=other_columns+list(hazard_class_dict.values())
        # Step 5: Reorder the columns using reindex with the desired_order followed by the remaining columns
        summary = summary.reindex(columns=desired_columns_order)
        return summary
    except Exception as e:
        raise Exception(f"Error in getSummary {str(e)}")

   # **********************BELOW FUNCTIONS ARE FOR RELATIVE VALUES, ***************************


def getSummaryRel(con, exposureid, column='areaOrLen', agg=False):
    try:
        if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
            raise ValueError(
                "column: status must be one of areaOrLen, value_exposure or population_exposure")
        metadata = readmeta.computeloss_meta(con, exposureid)
        exposure = readvector.prepareExposureForLoss(con, exposureid)
        hazid = metadata['hazid']
        classificationScheme = readmeta.classificationscheme(con, hazid)
        thresholds=classificationScheme['val1'].unique()
        thresholds.sort()
        min_thresholds=[float(val) for val in thresholds]
        costcol = metadata['costColumn']
        popcol = metadata['populColumn']
        type_col = metadata["TypeColumn"]
        if column == 'population_exposure':
            aggcolumn = popcol
        elif column == 'value_exposure':
            aggcolumn = costcol
        elif column == 'count':
            aggcolumn = "default_count"
        else:
            aggcolumn = 'areaOrLength'
        
        response,add_hazard_class_result,hazard_class_dict=add_hazard_class(exposure,min_thresholds,classificationScheme)
        if response:
            exposure=add_hazard_class_result
            exposure['default_count']=1
        else:
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result}")
        if not agg:
            summary = pd.pivot_table(exposure, values=column, index=[type_col],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            agg = exposure[[aggcolumn, type_col]].groupby(type_col).sum()
            summary = pd.merge(left=summary, right=agg,
                            left_on=type_col, right_on=type_col)
            summary[summary.columns.difference([aggcolumn])] = summary[summary.columns.difference([
                aggcolumn])].div(summary[aggcolumn], axis=0)*100
            summary = summary.reset_index()
            summary = summary.rename(columns={type_col: "Ear Class"})
        else:
            summary = pd.pivot_table(exposure, values=column, index=[type_col, 'admin_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            agg = exposure[[aggcolumn, type_col, 'admin_id']
                        ].groupby([type_col, 'admin_id']).sum()
            summary = pd.merge(summary, agg, 'left', on=['admin_id', type_col])
            summary[summary.columns.difference([aggcolumn])] = summary[summary.columns.difference([
                aggcolumn])].div(summary[aggcolumn], axis=0)*100

            summary = summary.reset_index()
            summary = summary.rename(
                columns={type_col: "Ear Class", "admin_id": "Admin Name"})
        summary = summary.fillna(0)
        #to drop column 0.0 if exists
        if 0.0 in summary.columns:
            summary = summary.drop(0.0, axis=1)
        # if 'areaOrLength' in summary.columns:
        #     summary = summary.drop('areaOrLength', axis=1)
        #Add missing hazard class in table
        for values in hazard_class_dict.values():
            if values not in summary.columns:
                summary[values]=0
        other_columns = [col for col in summary.columns if col not in hazard_class_dict.values()]
        desired_columns_order=other_columns+list(hazard_class_dict.values())
        # Step 5: Reorder the columns using reindex with the desired_order followed by the remaining columns
        summary = summary.reindex(columns=desired_columns_order)
        return summary
    except Exception as e:
        raise Exception(f"Error in getSummary {str(e)}")


def getShapefileRel(con, exposureid, column='areaOrLen', agg=False):
    try:
        if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
            raise ValueError(
                "column: status must be one of exposed %, areaOrLen, populationexp or valueexp")
        metadata = readmeta.computeloss_meta(con, exposureid)
        exposure = readvector.prepareExposureForLoss(con, exposureid)
        hazid = metadata["hazid"]
        classificationScheme = readmeta.classificationscheme(con, hazid)
        thresholds=classificationScheme['val1'].unique()
        thresholds.sort()
        min_thresholds=[float(val) for val in thresholds]
        type_col = metadata["TypeColumn"]
        adminid = metadata['adminid']
        earpk = metadata["earPK"]
        earid = metadata['earID']
        costcol = metadata['costColumn']
        popcol = metadata['populColumn']
        if column == 'population_exposure':
            aggcolumn = popcol
        elif column == 'value_exposure':
            aggcolumn = costcol
        elif column == 'count':
            aggcolumn = "default_count"
        else:
            aggcolumn = 'areaOrLength'
        response,add_hazard_class_result,hazard_class_dict=add_hazard_class(exposure,min_thresholds,classificationScheme)
        if response:
            exposure=add_hazard_class_result
            exposure['default_count']=1
        else:
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result}")
        if isinstance(add_hazard_class_result,dict):
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result['error']}")
        else:
            exposure=add_hazard_class_result
        # if column is count just count the number of feature exposed
        # if column == 'count':
        #     exposure[column] = 1

        if not agg:
            # this is always relative
            summary = pd.pivot_table(exposure, values=column, index=['geom_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)
            ear = readvector.readear(con, earid)
            summary = pd.merge(left=summary, right=ear,
                            left_on='geom_id', right_on=earpk, right_index=False)
            summary = gpd.GeoDataFrame(summary, geometry='geom')
        else:
            summary = pd.pivot_table(exposure, values=column, index=['admin_id'],
                                    columns=["class"], aggfunc=np.sum, fill_value=0)

            agg = exposure[[aggcolumn, 'admin_id']].groupby('admin_id').sum()
            summary = pd.merge(summary, agg, 'left', on=['admin_id'])
            summary[summary.columns.difference([aggcolumn])] = summary[summary.columns.difference([
                aggcolumn])].div(summary[aggcolumn], axis=0)*100
            admin = readvector.readAdmin(con, adminid)
            adminmeta = readmeta.getAdminMeta(con, adminid)
            adminpk = adminmeta.col_admin[0]
            summary = pd.merge(left=summary, right=admin,
                            left_on='admin_id', right_on=adminpk, right_index=False,how='right')
            summary = gpd.GeoDataFrame(summary, geometry='geom')
        summary = summary.fillna(0)
        #to drop column 0.0 if exists
        if 0.0 in summary.columns:
            summary = summary.drop(0.0, axis=1)
        # if 'areaOrLength' in summary.columns:
        #     summary = summary.drop('areaOrLength', axis=1)
        #Add missing hazard class in table
        for values in hazard_class_dict.values():
            if values not in summary.columns:
                summary[values]=0
        other_columns = [col for col in summary.columns if col not in hazard_class_dict.values()]
        desired_columns_order=other_columns+list(hazard_class_dict.values())
        # Step 5: Reorder the columns using reindex with the desired_order followed by the remaining columns
        summary = summary.reindex(columns=desired_columns_order)
        return summary
    except Exception as e:
        raise Exception(f"Error in getSummary {str(e)}")

def getGriddedExposureSummary(con, exposure_id, column='total_area_exposed', agg=False,agg_func=False):
    try:
        if column not in ['total_pixel_exposed', 'total_area_exposed', 'relative_exposed']:
            raise ValueError(
                "column: status must be one of total_pixel_exposed, total_area_exposed or relative_exposed")

        metadata = readmeta.computeloss_meta(con, exposure_id)
        schema=metadata['Schema']
        get_exp_data_success, get_exp_response = readvector.read_raster_exposure(con, exposure_id,schema)
        if get_exp_data_success:
            exposure=get_exp_response
        else:
             raise Exception(f"Error in get exposure data: {get_exp_response}")
         
        hazid = metadata['hazid']
        classificationScheme = readmeta.classificationscheme(con, hazid)
        thresholds=classificationScheme['val1'].unique()
        thresholds.sort()
        min_thresholds=[float(val) for val in thresholds]
        type_col = "ear_name" #metadata["TypeColumn"]
        response,add_hazard_class_result,hazard_class_dict=add_hazard_class(exposure,min_thresholds,classificationScheme,"hazard_name")
        if response:
            exposure=add_hazard_class_result
        else:
            raise Exception(f"Error in add_hazard_class_result: {add_hazard_class_result}")
            
        if agg:
            summary = pd.pivot_table(exposure, values=column, index=[type_col, 'admin_id'],
                                    columns=["hazard_name"],  fill_value=0) #aggfunc=np.sum,
            summary = summary.reset_index()
            summary = summary.rename(
                columns={type_col: "Ear Class", "admin_id": "Admin Name"})
        elif agg_func:
            summary = pd.pivot_table(exposure, values=column, index=[type_col],
                                    columns=["hazard_name"], aggfunc=np.sum, fill_value=0) #aggfunc=np.sum,
            summary = summary.reset_index()
            summary = summary.rename(
                columns={type_col: "Ear Class"})
        else:
            summary = pd.pivot_table(exposure, values=column, index=[type_col],
                                    columns=["hazard_name"], fill_value=0) #aggfunc=np.sum, 
            summary = summary.reset_index()
            summary = summary.rename(columns={type_col: "Ear Class"})
            
        summary = summary.fillna(0)
        #to drop column 0.0 if exists
        if 0.0 in summary.columns:
            summary = summary.drop(0.0, axis=1)
        #Add missing hazard class in table
        for values in hazard_class_dict.values():
            if values not in summary.columns:
                summary[values]=0
        other_columns = [col for col in summary.columns if col not in hazard_class_dict.values()]
        desired_columns_order=other_columns+list(hazard_class_dict.values())
        # Step 5: Reorder the columns using reindex with the desired_order followed by the remaining columns
        summary = summary.reindex(columns=desired_columns_order)
        return True, summary
    except Exception as e:
        return False, str(e)
        # raise Exception(f"Error in getSummary {str(e)}")
