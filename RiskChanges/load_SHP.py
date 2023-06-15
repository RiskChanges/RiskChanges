import geopandas
from sqlalchemy import *


def loadshp(shpInput, engine, lyrName, schema, index,calcAreaOrLen=False):
    try:
        gdf = geopandas.read_file(shpInput)
        gdf = gdf[gdf.is_valid]
        gdf[index] = gdf.index
        gdf.to_postgis(name=lyrName, con=engine,
                                schema=schema, if_exists='replace')
        if calcAreaOrLen:
            geometrytype = gdf.geom_type.unique()[0]
            with engine.connect() as connection:
                connection.execute('''ALTER TABLE "{0}"."{1}" Add COLUMN "areaOrLength" float'''.format(schema,lyrName))
                if (geometrytype == 'Polygon' or geometrytype == 'MultiPolygon'):
                    connection.execute('''UPDATE "{0}"."{1}" SET "areaOrLength" = ST_Area(geometry)'''.format(schema,lyrName))
                elif(geometrytype == 'LineString' or geometrytype == 'MultiLineString'):
                    connection.execute('''UPDATE "{0}"."{1}" SET "areaOrLength" = ST_Length(geometry)'''.format(schema,lyrName))
                elif(geometrytype == 'Point' or geometrytype == 'MultiPoint'):
                    connection.execute('''UPDATE "{0}"."{1}" SET "areaOrLength" = 1'''.format(schema,lyrName))
                else:
                    return {"error":f"Invalid geometrytype {geometrytype}"}  
        # engine.dispose()
        return "successful"
    except Exception as e:
        return {"error":str(e)}
