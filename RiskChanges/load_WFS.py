import geopandas as gpd
import requests
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
from owslib.wfs import WebFeatureService


def LoadWFS(wfsURL, layer_name, connstr, layer_name_db, index, schema):

    params = dict(service='WFS', version="1.0.0", request='GetFeature',
                  typeName=layer_name, outputFormat='application/json')

    u = requests.get(wfsURL, params)
    data_url = u.url

    # Read data from URL
    gdf = gpd.read_file(data_url)
    gdf = gdf[gdf.is_valid]
    gdf[index] = gdf.index

    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)
    gdf.to_postgis(name=layer_name_db, con=engine,
                   schema=schema, if_exists='replace')

    # close engine
    engine.dispose()


# LoadWFS(wfsURL="http://geo.stat.fi/geoserver/vaestoruutu/wfs",layer_name='vaestoruutu:vaki2005_1km_kp',connstr='postgresql://postgres:puntu@localhost:5432/postgres',lyrName='Tekson_WFS',schema='tekson')

# LoadWFS(r'http://tajirisk.ait.ac.th:8080/geoserver/ows',
#         'tajikistan:region', 'postgresql://postgres:gicait123@203.159.29.45:5432/sdssv2', 'kamal', 'geoinformatics_center')


def test_WFS(wfs_url, layer_name):
    wfs11 = WebFeatureService(url=wfs_url, version='1.1.0')
    wfs_layers = list(wfs11.contents)

    if layer_name in wfs_layers:
        return layer_name

    else:
        return "Layer not available. Available layers are: " + wfs_layers
