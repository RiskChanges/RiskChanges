import geopandas as gpd
import requests
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
from owslib.wfs import WebFeatureService
from .utils import get_geom_type,is_utm_epsg,utm_finder

def LoadWFS(wfsURL, layer_name, connstr, layer_name_db, index, schema):
    #TODO if gdf is not projected, convert it
    try:
        params = dict(service='WFS', version="1.0.0", request='GetFeature',
                    typeName=layer_name, outputFormat='application/json')
        u = requests.get(wfsURL, params)
        data_url = u.url
        
        # Read data from URL
        gdf = gpd.read_file(data_url)
        gdf = gdf[gdf.is_valid]
        
        #Get geometry type
        geometry_types= gdf.geom_type.unique()
        success,response=get_geom_type(geometry_types)
        if success:
            geometry_type=response
        else:
            return False, response, None
        
        #Handle CRS
        gdf_crs = gdf.crs.to_epsg()
        is_UTM_epsg=is_utm_epsg(gdf_crs)
        if not is_UTM_epsg:
            gdf_bbox  = gdf.total_bounds
            min_x, min_y, max_x, max_y = gdf_bbox
            bbox=[min_x, min_y, max_x, max_y]
            success,dst_utm_epsg=utm_finder(gdf_crs,bbox)
            if not success:
                error=str(dst_utm_epsg)
                return False,error,None
            target_crs = CRS.from_epsg(dst_utm_epsg).to_string()
            gdf = gdf.to_crs(target_crs)
        
        gdf[index] = gdf.index
        # Creating SQLAlchemy's engine to use
        engine = create_engine(connstr)
        gdf.to_postgis(name=layer_name_db, con=engine,
                    schema=schema, if_exists='replace')
        # close engine
        engine.dispose()
        return True, "successful",geometry_type
    except Exception as e:
        return False, str(e),None


# LoadWFS(wfsURL="http://geo.stat.fi/geoserver/vaestoruutu/wfs",layer_name='vaestoruutu:vaki2005_1km_kp',connstr='postgresql://postgres:puntu@localhost:5432/postgres',lyrName='Tekson_WFS',schema='tekson')

# LoadWFS(r'http://tajirisk.ait.ac.th:8080/geoserver/ows',
#         'tajikistan:region', 'postgresql://postgres:gicait123@203.159.29.45:5432/sdssv2', 'kamal', 'geoinformatics_center')

# not in use in riskchanges backend
def test_WFS(wfs_url, layer_name):
    try:
        wfs11 = WebFeatureService(url=wfs_url, version='1.1.0')
        wfs_layers = list(wfs11.contents)

        if layer_name in wfs_layers:
            return True, layer_name

        else:
            return False , "Layer not available. Available layers are: " + wfs_layers
    except Exception as e:
        return False , str(e)
        
        
