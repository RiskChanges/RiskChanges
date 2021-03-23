#!/usr/bin/env python
# coding: utf-8

# In[6]:


import psycopg2
import pandas as pd
import numpy
import geopandas as gpd
import random
import string
import pandas.io.sql as sqlio
# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine


def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
#from .classifyVulnerability import classifyVulnerability


# In[7]:


class Loss:
    def __init__(self, exposureID, exposureIndexTableName, hazardIndexTableName, earIndexTableName, aggrigationColumn, vulnColn, schema, organizationSchema):
        self.exposureID = exposureID
        self.exposureIndexTableName = exposureIndexTableName
        self.hazardIndexTableName = hazardIndexTableName
        self.earIndexTableName = earIndexTableName
        self.aggrigationColumn = aggrigationColumn
        self.schema = schema
        self.organizationSchema = organizationSchema
        self.vulnColumn = vulnColn
        self.spprob = 1
        self.Con = None
        self.earPK = None
        self.hazIndex = None
        self.earIndex = None
        self.exposureTable = None
        self.costColumn = None
        self.typeColumn = None  # get from dbase make it none
        self.eartable = None
        self.exposuretable = None
        self.losstable = None
        self.losstableAgg = None

    def createCon(self, connstr):
        self.Con = psycopg2.connect(connstr)

    def getExposureMeta(self):
        sql_load_exp = '''SELECT * FROM "{}"."{}" WHERE id={}'''.format(
            self.schema, self.exposureIndexTableName, str(self.exposureID))
        exp_meta_df = pd.read_sql_query(sql_load_exp, self.Con)

        '''
        index_haz_col=colnames.index("hazard_index")
        index_ear_col=colnames.index("ear_index")
        index_expo_col=colnames.index("exposure_table")
        index_ear_pk_col=colnames.index("ear_index_primary_key")
        '''

        self.earPK = exp_meta_df["ear_index_id"][0]
        self.hazIndex = exp_meta_df["hazard_index_id"][0]
        self.earIndex = exp_meta_df["ear_index_id"][0]
        self.exposureTable = exp_meta_df["exposure_table"][0]
        # save EAR table name, Hazard Name, classification scheme, Exposure table

    def getEarTableMeta(self, cal_type):
        sql_ear = '''SELECT * FROM "{}"."{}" WHERE id={}'''.format(
            self.schema, self.earIndexTableName, self.earIndex)

        ear_index = pd.read_sql_query(sql_ear, self.Con)

        if cal_type == 'population':
            self.costColumn = ear_index['col_population_avg'][0]

        elif cal_type == 'value':
            self.costColumn = ear_index['col_value_avg'][0]

        else:
            raise ValueError(
                "The loss calculation must be either cost or population")

        self.typeColumn = ear_index['col_class'][0]
        self.earTable = ear_index['table_name'][0]
    # get information such as cost column name, Type column name, ear table name

    def getHazMeta(self):
        sql_haz = 'SELECT * From '+self.schema+'."'+self.hazardIndexTableName + \
            '" WHERE id='+str(self.hazIndex)  # write proper sql

        sql_haz = '''SELECT * FROM "{}"."{}" WHERE id={}'''.format(
            self.schema, self.hazardIndexTableName, str(self.hazIndex))
        haz_meta = pd.read_sql_query(sql_haz, self.Con)

        self.hazunit = haz_meta['unit'][0]
        self.intensity = haz_meta['intensity'][0]

    def getEarData(self):
        sql_ear = '''SELECT id, "{0}", "{1}", "{2}" FROM {3}.{4}'''.format(
            self.costColumn, self.typeColumn, self.vulnColumn, self.organizationSchema, self.earTable)
        self.eartable = pd.read_sql_query(sql_ear, self.Con)
    # get information such as ID, cost, type etc

    def getExposureData(self):
        sql_exposure = '''SELECT  "class", "exposed", "geom_id", "exposure_id" FROM "{0}"."{1}"'''.format(
            self.organizationSchema, self.exposureTable)
        exposure = pd.read_sql_query(sql_exposure, self.Con)
        self.exposuretable = pd.merge(left=exposure, right=self.eartable, how='left', left_on=[
                                      'geom_id'], right_on=['id'])

    def getHazardMeanIntensity(self):
        stepsize = 5  # import from database
        base = 0  # import from database
        half_step = stepsize/2
        self.exposuretable['meanHazard'] = base + \
            self.exposuretable['class']*stepsize-half_step

    def checknconvert(self):
        # if vulnerability and hazard do not have same units convert mean hazard to same values..
        if self.hazunit == self.vulnunit:
            self.exposuretable['meanHazard'] = self.exposuretable['meanHazard']
        else:
            sql_mulfactor = '''SELECT * FROM public.unitconv WHERE fromunit={} AND tounit={}'''.format(
                self.hazunit, self.vulnunit)
            mulfactor = sql_mulfactor['multiplyfactor'][0]
            self.exposuretable['meanHazard'] = self.exposuretable['meanHazard']*mulfactor

    def getVulnerability(self, haztype):
        if haztype == "Intensity":
            final_df = pd.DataFrame()
            for i in self.exposuretable[self.vulnColumn].unique():
                sql_vul_vals = '''SELECT * FROM public."projectIndex_vulvalues" WHERE "vulnID_fk_id"={}'''.format(
                    str(i))
                vulnerbaility = pd.read_sql_query(sql_vul_vals, self.Con)
                vulnerbaility['mean_x'] = vulnerbaility.apply(lambda row: (
                    row.hazIntensity_from+row.hazIntensity_to)/2, axis=1)
                Y = vulnerbaility.iloc[:, 4].values
                X = vulnerbaility.iloc[:, 6:7].values

                poly = PolynomialFeatures(degree=10)

                X_poly = poly.fit_transform(X)
                pol_reg = LinearRegression()

                pol_reg.fit(X_poly, Y)
                subset_exp = self.exposuretable[self.exposuretable[self.vulnColumn] == i]
                subset_exp["vuln"] = subset_exp.apply(lambda row: pol_reg.predict(
                    poly.fit_transform([[row.meanHazard]]))[0], axis=1)
                final_df = final_df.append(subset_exp, ignore_index=True)
            self.exposuretable = None
            self.exposuretable = final_df

        elif haztype == "susceptibility":
            for i in self.exposuretable[self.vulnColumn].unique():
                sql_vul_vals = '''SELECT * FROM public."projectIndex_vulvalues" WHERE "vulnID_fk_id" ={}'''.format(
                    str(i))
                vulnerbaility = pd.read_sql_query(sql_vul_vals, self.Con)
                subset_exp = self.exposuretable[self.exposuretable[self.vulnColumn] == i]

                subset_exp = pd.merge(left=subset_exp, right=vulnerbaility[[
                                      'vulnAVG', 'hazIntensity_to']], how='left', left_on=['class'], right_on=['hazIntensity_to'])
                subset_exp.drop(columns=['hazIntensity_to'])
                subset_exp.rename(columns={"vulnAVG": "vuln"})
                final_df = final_df.append(subset_exp, ignore_index=True)
            self.exposuretable = None
            self.exposuretable = final_df

    def spatial_overlay(self, aggregateon):
        sql_agg = '''SELECT {0}.*, {1}.{2} FROM {0}, {1} WHERE ST_Intersects({0}.geom, {1}.geom)'''.format(
            self.earTable, aggregateon, self.aggrigationColumn)

        eartemp = pd.read_sql_query(sql_agg, self.Con)
        self.exposuretable = pd.merge(left=self.exposuretable, right=eartemp[[
                                      self.aggrigationColumn, 'gid']], how='left', left_on=['geom_id'], right_on=['gid'])
        self.exposuretable.rename(
            columns={self.aggrigationColumn: "admin_unit"})

    def computeLoss(self, aggregation):
        if aggregation:
            if 'admin_unit' not in self.exposuretable.columns:
                raise ValueError(
                    "Aggregation must required in the exposure table")

            self.exposuretable['loss'] = self.exposuretable.apply(
                lambda row: row[self.costColumn]*row.exposed*row.vuln*self.spprob/100, axis=1)

            self.losstable = self.exposuretable.groupby(
                ["id"], as_index=False).agg({'loss': 'sum'})
            self.losstableAgg = self.exposuretable.groupby(
                ['admin_unit'], as_index=False).agg({'loss': 'sum'})
        else:
            self.exposuretable['loss'] = self.exposuretable.apply(
                lambda row: row[self.costColumn]*row.exposed*row.vuln*self.spprob/100, axis=1)

            self.losstable = self.exposuretable.groupby(
                ["id"], as_index=False).agg({'loss': 'sum'})

    def saveLoss(self, constr, lossID, losstable, aggregated):
        engine = create_engine(constr)
        if aggregated:
            loss_table_agg = losstable+"_agg"
            self.losstable["lossID"] = lossID
            self.losstableAgg["lossID"] = lossID
            self.losstable.to_sql(
                losstable, engine, self.organizationSchema, if_exists='append', index=False)
            self.losstableAgg.to_sql(
                loss_table_agg, engine, self.organizationSchema, if_exists='append', index=False)
        else:
            self.losstable["lossID"] = lossID
            self.losstable.to_sql(
                losstable, engine, self.organizationSchema, if_exists='append', index=False)


def main(exposureID, exposureIndexTableName, hazardIndexTableName, earIndexTableName, aggrigationColumn, vulnColn, schema, organizationSchema, connstr, cal_type, preaggregated, lossid, losstable, aggregate, aggregateon=None):
    lossA = Loss(exposureID, exposureIndexTableName, hazardIndexTableName,
                 earIndexTableName, aggrigationColumn, vulnColn, schema)
    lossA.createCon(connstr)
    lossA.getExposureMeta()
    lossA.getEarTableMeta(cal_type)
    lossA.getHazMeta()
    lossA.getEarData()
    lossA.getExposureData()
    lossA.getHazardMeanIntensity()

    if lossA.intensity == 'susceptibility':
        lossA.getVulnerability("susceptibility")
    else:
        lossA.checknconvert()
        lossA.getVulnerability("Intensity")
    if not aggregate:
        lossA.computeLoss(aggregate)
    else:
        if preaggregated:
            lossA.computeLoss(aggregate)
        else:
            lossA.spatial_overlay(aggregateon, aggrigationColumn)
            lossA.computeLoss(aggregate)
    lossA.saveLoss(connstr, lossid, losstable, aggregate)


# if __name__ == '__main__':
#     main(exposureID, exposureIndexTableName, hazardIndexTableName, earIndexTableName,
#          costColumn, typeColumn, aggrigationColumn, vulnColn, Schema, connstr)


# In[8]:


exposureID = 320
exposureIndexTableName = "exposure_exposureindex"
earIndexTableName = "projectIndex_earindex"
hazardIndexTableName = "projectIndex_hazardindex"
aggrigationColumn = "admin_unit"
vulnColn = "Flash flood"
schema = "public"
organizationSchema = 'geoinformatics_center'


# In[9]:


lossA = Loss(exposureID, exposureIndexTableName, hazardIndexTableName,
             earIndexTableName, aggrigationColumn, vulnColn, schema, organizationSchema)


# In[10]:


lossA.createCon("postgresql://postgres:gicait123@203.159.29.45:5432/sdssv2")
lossA.getExposureMeta()
lossA.getEarTableMeta('value')
lossA.getHazMeta()
lossA.getEarData()
lossA.getExposureData()
lossA.getHazardMeanIntensity()
lossA.getVulnerability("Intensity")
lossA.computeLoss(False)
lossA.saveLoss("postgresql://postgres:gicait123@203.159.29.45:5432/sdssv2",
               565, 'random_loss', False)
