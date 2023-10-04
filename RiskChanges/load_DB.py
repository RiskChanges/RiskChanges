import geopandas as gpd
from sqlalchemy.engine import create_engine
from geoalchemy2 import Geometry, WKTElement
from .utils import get_geom_type,is_utm_epsg,utm_finder

def load_db(query, source_connection, destination_connection, destination_table, destination_schema, index):
    #TODO if df.crs is not projected, convert it
    try:
        try:
            gdf = gpd.read_postgis(query, source_connection,geom_col='geometry')
        except:
            gdf = gpd.read_postgis(query, source_connection,geom_col='geom')
            gdf=gdf.rename( columns={"geom":"geometry"})
        if gdf.empty:
            return False, "Geometry column must be named with geom or geometry", None
        crs = gdf.crs
        
        geometry_types=gdf.geometry.geom_type.unique()
        success,response=get_geom_type(geometry_types)
        if success:
            geometry_type=response
        else:
            return False, response, None
            # geometry_type="Geometry"
        # try:
        #     epsg = crs.to_epsg()
        # except:
        #     print('Warning! Coordinate system manually assigned to 4326. This might affect on visualization of data.')
        #     epsg = 4326
            
        #Handle CRS
        gdf_crs = gdf.crs.to_epsg()
        
        print(gdf_crs,"ccccccccccccccccccccccccccccccccrrrrrrrrrrrrrrrrrrrsssssssssssssssssssssss")
        is_UTM_epsg=is_utm_epsg(gdf_crs)
        if is_UTM_epsg:
            print("Is utm epsg")
            dst_utm_epsg=gdf_crs
        else:
            print(gdf_crs,"ccccccccccccccccccccccccccccccccrrrrrrrrrrrrrrrrrrrsssssssssssssssssssssss######")
            gdf_bbox  = gdf.total_bounds
            min_x, min_y, max_x, max_y = gdf_bbox
            bbox=[min_x, min_y, max_x, max_y]
            success,dst_utm_epsg=utm_finder(gdf_crs,bbox)
            if not success:
                error=str(dst_utm_epsg)
                return False,error,None
            target_crs = CRS.from_epsg(dst_utm_epsg).to_string()
            gdf = gdf.to_crs(target_crs)    
        
        gdf['geometry'] = gdf['geometry'].apply(
            lambda x: WKTElement(x.wkt, srid=dst_utm_epsg))
        gdf[index] = gdf.index
        engine = create_engine(destination_connection)
        gdf.to_sql(destination_table, engine, schema=destination_schema, if_exists='replace', index=False,
                dtype={'geometry': Geometry(geometry_type=geometry_type, srid=dst_utm_epsg)}) #GEOMETRY
        engine.dispose()
        return True, "success",geometry_type
    except Exception as e:
        return False, str(e),None
