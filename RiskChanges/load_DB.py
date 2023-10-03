import geopandas as gpd
from sqlalchemy.engine import create_engine
from geoalchemy2 import Geometry, WKTElement

def load_db(query, source_connection, destination_connection, destination_table, destination_schema, index):
    #TODO if df.crs is not projected, convert it
    try:
        try:
            df = gpd.read_postgis(query, source_connection,geom_col='geometry')
        except:
            df = gpd.read_postgis(query, source_connection,geom_col='geom')
            df=df.rename( columns={"geom":"geometry"})
        if df.empty:
            return False, "Geometry column must be named with geom or geometry"
        crs = df.crs
        try:
            epsg = crs.to_epsg()
        except:
            print('Warning! Coordinate system manually assigned to 4326. This might affect on visualization of data.')
            epsg = 4326
        df['geometry'] = df['geometry'].apply(
            lambda x: WKTElement(x.wkt, srid=epsg))
        df[index] = df.index
        engine = create_engine(destination_connection)
        df.to_sql(destination_table, engine, schema=destination_schema, if_exists='replace', index=False,
                dtype={'geometry': Geometry(geometry_type='GEOMETRY', srid=epsg)})
        engine.dispose()
        return True, "success"
    except Exception as e:
        return False, str(e)
