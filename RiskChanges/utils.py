import rasterio
from pyproj import CRS
from pyproj.database import query_utm_crs_info
from pyproj.aoi import AreaOfInterest

def get_geom_type(geom_types):
    '''
    geom_types: list of unique geom types
    At present only support point, line or polygon
    '''
    geometrytype=None
    try:
        if len(geom_types) == 1:
            geometrytype = geom_types[0]
            if geometrytype== "Point" or  geometrytype== "MultiPoint":
                geometrytype='point'
            elif geometrytype== "LineString" or  geometrytype== "MultiLineString":
                geometrytype='line'
            elif geometrytype== "Polygon" or  geometrytype== "MultiPolygon":
                geometrytype='polygon'
            else:
                return False, f"invalid geom_type {geom_types}"
        elif len(geom_types) == 2:
            if "Point" and "MultiPoint" in geom_types:
                geometrytype = "point"
            elif "LineString" and "MultiLineString" in geom_types:
                geometrytype = "line"
            elif "Polygon" and "MultiPolygon" in geom_types:
                geometrytype = "polygon"
            else:
                return False,f"error occured : more than one geometry type is found i.e. {geom_types}"
        elif len(geom_types) > 2:
            return False, f"error occured : more than one geometry type is found i.e. {geom_types}"
        else:
            return False, f"error occured : missing geometry"
        return True, geometrytype
    except Exception as e:
        return False, str(e)
    
def is_utm_epsg(epsg_code):
    try:
        crs = CRS.from_epsg(epsg_code)
        return crs.coordinate_operation.method_name == "Transverse Mercator"
    except Exception as e:
        return False

def utm_finder(src_crs,bbox=[]):
    try:
        """
        Find UTM epsg
        raster: input raster path
        Returns:
        UTM EPSG code of the input raster
        """
        # with rasterio.open(raster_file_path) as dataset:  
            # src_epsg=dataset.crs.to_epsg()
            # bbox  = dataset.bounds
        bbox_wgs84 = rasterio.warp.transform_bounds(src_crs,'EPSG:4326', bbox[0],bbox[1],bbox[2],bbox[3])
        utm_crs_list = query_utm_crs_info(     
            datum_name='WGS 84',
            area_of_interest= AreaOfInterest(
            west_lon_degree=bbox_wgs84[0],
            south_lat_degree=bbox_wgs84[1],
            east_lon_degree=bbox_wgs84[2],
            north_lat_degree=bbox_wgs84[3],),) 

        # utm_crs = '{}:{}'.format(utm_crs_list[0].auth_name,utm_crs_list[0].code)
        utm_epsg = utm_crs_list[0].code
        return True,utm_epsg
    except Exception as e:
        return False, str(e)
