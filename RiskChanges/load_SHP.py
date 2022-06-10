import geopandas
from sqlalchemy import *


def loadshp(shpInput, connstr, lyrName, schema, index):
    gdf = geopandas.read_file(shpInput)

    gdf = gdf[gdf.is_valid]

    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)

    gdf[index] = gdf.index

    gdf.to_postgis(name=lyrName, con=engine,
                            schema=schema, if_exists='replace')

    engine.dispose()
