import geopandas
from sqlalchemy import *


def loadshp(shpInput, connstr, lyrName, schema, index):
    geodataframe = geopandas.read_file(shpInput)

    # Creating SQLAlchemy's engine to use
    engine = create_engine(connstr)

    geodataframe[index] = geodataframe.index

    geodataframe.to_postgis(name=lyrName, con=engine,
                            schema=schema, if_exists='replace')

    engine.dispose()
