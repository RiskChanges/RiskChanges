import geopandas as gpd
import pandas as pd
import numpy as np

from .RiskChangesOps.readvulnerability import readIntVuln, readSusVuln
from .RiskChangesOps import readmeta, readvector, writevector, AggregateData as aggregator


def getSummary(con, exposureid, column='areaOrLen', agg=False):

    if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
        raise ValueError(
            "column: status must be one of areaOrLen, value_exposure or population_exposure")
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata['hazid']
    classificationScheme = readmeta.classificationscheme(con, hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(
        start=base, stop=maxval+1, step=stepsize).tolist()
    convert_dict = {}
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure['class'].replace(convert_dict, inplace=True)

    # if column is count just count the number of feature exposed
    if column == 'count':
        exposure[column] = 1

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
    return summary


def getShapefile(con, exposureid, column='exposed', agg=False):
    if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
        raise ValueError(
            "column: status must be one of areaOrLen, value_exposure or population_exposure")
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata["hazid"]
    classificationScheme = readmeta.classificationscheme(con, hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    adminid = metadata['adminid']
    earpk = metadata["earPK"]
    earid = metadata['earID']
    earid = metadata['earID']
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(
        start=base, stop=maxval+1, step=stepsize).tolist()
    convert_dict = {}
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure["class"].replace(convert_dict, inplace=True)

    # if column is count just count the number of feature exposed
    if column == 'count':
        exposure[column] = 1

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
                           left_on='admin_id', right_on=adminpk, right_index=False)
        summary = gpd.GeoDataFrame(summary, geometry='geom')

    return summary

   # **********************BELOW FUNCTIONS ARE FOR RELATIVE VALUES, ***************************


def getSummaryRel(con, exposureid, column='areaOrLen', agg=False):

    if column not in ['areaOrLen', 'value_exposure', 'population_exposure', 'count']:
        raise ValueError(
            "column: status must be one of areaOrLen, value_exposure or population_exposure")
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata['hazid']
    classificationScheme = readmeta.classificationscheme(con, hazid)
    costcol = metadata['costColumn']
    popcol = metadata['populColumn']
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(
        start=base, stop=maxval+1, step=stepsize).tolist()
    convert_dict = {}
    if column == 'population_exposure':
        aggcolumn = popcol
    elif column == 'value_exposure':
        aggcolumn = costcol
    else:
        aggcolumn = 'areaOrLen'
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure['class'].replace(convert_dict, inplace=True)

    # if column is count just count the number of feature exposed
    if column == 'count':
        exposure[column] = 1

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
    return summary


def getShapefileRel(con, exposureid, column='exposed', agg=False):
    if column not in ['exposed', 'areaOrLen', 'value_exposure', 'population_exposure', 'count']:
        raise ValueError(
            "column: status must be one of exposed %, areaOrLen, populationexp or valueexp")
    metadata = readmeta.computeloss_meta(con, exposureid)
    exposure = readvector.prepareExposureForLoss(con, exposureid)
    hazid = metadata["hazid"]
    classificationScheme = readmeta.classificationscheme(con, hazid)
    base = float(metadata["base"])
    maxval = float(metadata["threshold"])
    type_col = metadata["TypeColumn"]
    adminid = metadata['adminid']
    earpk = metadata["earPK"]
    earid = metadata['earID']
    earid = metadata['earID']
    stepsize = float(metadata["stepsize"])
    min_thresholds = np.arange(
        start=base, stop=maxval+1, step=stepsize).tolist()
    convert_dict = {}
    costcol = metadata['costColumn']
    popcol = metadata['populColumn']
    if column == 'population_exposure':
        aggcolumn = popcol
    elif column == 'value_exposure':
        aggcolumn = costcol
    else:
        aggcolumn = 'areaOrLen'
    for i, val in enumerate(min_thresholds):

        # the default type for val1 is char, change it to float and compare
        classificationScheme['val1'] = classificationScheme['val1'].astype(
            float)
        name = classificationScheme.query(f'val1 == {val}')

        # not every hazard class are avialable on exposure table
        # so try except to pass even the class is not available
        try:
            name = name['class_name'].to_list()[0]
            convert_dict[i] = name

        except:
            pass

        # if it is last class, then need to assign max class for all result
        if (val == min_thresholds[-1]):
            exposure['class'] = np.where(
                exposure['class'] >= i, i, exposure['class'])

    # Change the classes to the user defined class
    exposure["class"].replace(convert_dict, inplace=True)

    # if column is count just count the number of feature exposed
    if column == 'count':
        exposure[column] = 1

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
                           left_on='admin_id', right_on=adminpk, right_index=False)
        summary = gpd.GeoDataFrame(summary, geometry='geom')

    return summary
