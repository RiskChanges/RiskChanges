import geopandas as gpd
from .RiskChangesOps.readraster import readhaz
from .RiskChangesOps.readvector import readear, readAdmin, readLoss, readexposure, readRiskGeometry, readexposureGeom, readLossGeom
from .RiskChangesOps import readmeta


import string
import random


def name_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def getEARJSON(con, earid, storedir='/temp/'):
    ear = readear(con, earid)
    filename = name_generator()
    filepath = f"{storedir}{filename}.geojson"
    try:
        ear.to_file(filepath, driver='GeoJSON')
    except:
        print(
            f"couldn't save the file at {filepath} please check filename directory and required permissions")
    return filepath


def getExposureJSON(con, exposureid, storedir='/temp/'):
    exposure_geom = readexposureGeom(con, exposureid)
    filename = name_generator()
    filepath = f"{storedir}{filename}.geojson"
    try:
        exposure_geom.to_file(filepath, driver='GeoJSON')
    except:
        print(
            f"couldn't save the file at {filepath} please check filename directory and required permissions")
    return filepath


def getLossJSON(con, lossid, storedir='/temp/'):
    loss_geom = readLossGeom(con, lossid)
    filename = name_generator()
    filepath = f"{storedir}{filename}.geojson"
    try:
        loss_geom.to_file(filepath, driver='GeoJSON')
    except:
        print(
            f"couldn't save the file at {filepath} please check filename directory and required permissions")
    return filepath


def getRiskJSON(con, riskid, storedir='/temp/'):
    risk_geom = readRiskGeometry(con, riskid)
    filename = name_generator()
    filepath = f"{storedir}{filename}.geojson"
    try:
        risk_geom.to_file(filepath, driver='GeoJSON')
    except:
        print(
            f"couldn't save the file at {filepath} please check filename directory and required permissions")
    return filepath
