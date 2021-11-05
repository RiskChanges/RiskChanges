import geopandas
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *


def loadshp(shpInput, connstr, lyrName, schema, index):
    geodataframe = geopandas.read_file(shpInput)
    crs = geodataframe.crs
    try:
        epsg = crs.to_epsg()

    except:
        print('Warning! Coordinate system manually assigned to 4326. This might affect on visualization of data.')
        epsg = 4326

    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)

    geodataframe[index] = geodataframe.index
    if 'geometry' not in geodataframe.columns:
        raise ValueError(
            "The input shapefile do not consist of 'geometry' column, please make sure it has geometry column (it can be with different name such as geom, in such case please rename it.)")

    geodataframe['geom'] = geodataframe['geometry'].apply(
        lambda x: WKTElement(x.wkt, srid=epsg))

    # drop the geometry column as it is now duplicative
    geodataframe.drop('geometry', 1, inplace=True)

    geodataframe.to_sql(lyrName, engine, schema, if_exists='replace', index=False,
                        dtype={'geom': Geometry('Geometry', srid=epsg)})
    engine.dispose()


#     crs_name = str(geodataframe.crs.srs)
#     print(geodataframe.crs, 'crs')
#     print(geodataframe.crs.to_epsg(), type(geodataframe.crs.to_epsg()), 'epsg')
#     print(geodataframe.crs.srs, 'srs called')
#     print(type(geodataframe.crs))
