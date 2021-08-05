import geopandas
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *


def loadshp(shpInput, connstr, lyrName, schema, index):
    print('loadshp called')
    # Load data in geodataframe
    geodataframe = geopandas.read_file(shpInput)

    # Identify CRS
    # return  geodataframe
    crs_name = str(geodataframe.crs.srs)
    try:
        epsg = int(crs_name.replace('epsg:', ''))

    except:
        epsg = None

    if epsg is None:
        epsg = 4326

    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)

    geodataframe[index] = geodataframe.index

    # ... [do something with the geodataframe]

    geodataframe['geom'] = geodataframe['geometry'].apply(
        lambda x: WKTElement(x.wkt, srid=epsg))

    # drop the geometry column as it is now duplicative
    geodataframe.drop('geometry', 1, inplace=True)

    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'
    geodataframe.to_sql(lyrName, engine, schema, if_exists='replace', index=False,
                        dtype={'geom': Geometry('Geometry', srid=epsg)})
    engine.dispose()
