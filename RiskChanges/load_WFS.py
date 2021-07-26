import geopandas as gpd
import requests
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *


def LoadWFS(wfsURL, layer_name, connstr, layer_name_db, index, schema):

    params = dict(service='WFS', version="1.0.0", request='GetFeature',
                  typeName=layer_name, outputFormat='application/json')

    u = requests.get(wfsURL, params)

    data_url = u.url

    # Read data from URL
    geodataframe = gpd.read_file(data_url)

    crs_name = str(geodataframe.crs.srs)
    epsg = int(crs_name.replace('epsg:', ''))
    if epsg is None:
        epsg = 4326

    geodataframe[index] = geodataframe.index
    engine = create_engine(connstr)

    geodataframe['geom'] = geodataframe['geometry'].apply(
        lambda x: WKTElement(x.wkt, srid=epsg))

    # drop the geometry column as it is now duplicative
    geodataframe.drop('geometry', 1, inplace=True)

    geodataframe.to_sql(layer_name_db, engine, schema=schema, if_exists='replace', index=False,
                        dtype={'geom': Geometry('Geometry', srid=epsg)})
    engine.dispose()


# LoadWFS(wfsURL="http://geo.stat.fi/geoserver/vaestoruutu/wfs",layer_name='vaestoruutu:vaki2005_1km_kp',connstr='postgresql://postgres:puntu@localhost:5432/postgres',lyrName='Tekson_WFS',schema='tekson')

# LoadWFS(r'http://tajirisk.ait.ac.th:8080/geoserver/ows',
#         'tajikistan:region', 'postgresql://postgres:gicait123@203.159.29.45:5432/sdssv2', 'kamal', 'geoinformatics_center')
