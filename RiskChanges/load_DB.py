import geopandas as gpd
from sqlalchemy.engine import create_engine
from geoalchemy2 import Geometry, WKTElement


def load_db(query, source_connection, destination_connection, destination_table, destination_schema, index):

    df = gpd.read_postgis(query, source_connection)

    crs = df.crs
    try:
        epsg = crs.to_epsg()

    except:
        print('Warning! Coordinate system manually assigned to 4326. This might affect on visualization of data.')
        epsg = 4326

    df['geom'] = df['geom'].apply(
        lambda x: WKTElement(x.wkt, srid=epsg))

    df[index] = df.index
    print(df.head())

    engine = create_engine(destination_connection)

    df.to_sql(destination_table, engine, schema=destination_schema, if_exists='replace', index=False,
              dtype={'geom': Geometry('Geometry', srid=epsg)})

    engine.dispose()
