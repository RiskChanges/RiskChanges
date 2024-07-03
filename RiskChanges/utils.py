import rasterio
from pyproj.database import query_utm_crs_info
from pyproj.aoi import AreaOfInterest
from rasterio.warp import calculate_default_transform, Resampling,reproject
from pyproj import CRS,Transformer
from rasterio.windows import Window
import numpy as np
from shapely.geometry import box
# from osgeo import gdal, osr, ogr
from rasterio.transform import Affine
from osgeo import gdal, osr, ogr

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
    
def utm_epsg_bbox(utm_epsg_code):
    try:
        crs_utm = CRS.from_user_input(int(utm_epsg_code))
        bbox=crs_utm.area_of_use.bounds
        return True, bbox
    except Exception as e:
        return False, str(e)

 
def intersected_area(bbox1, bbox2):
    """
    Calculate the intersected area of two bounding boxes.
    
    Parameters:
    - bbox1: A tuple representing the first bounding box (min_x, min_y, max_x, max_y).
    - bbox2: A tuple representing the second bounding box (min_x, min_y, max_x, max_y).
    
    Returns:
    - area: The intersected area of the two bounding boxes.
    """
    try:
        # Create shapely box objects from the bounding boxes
        box1 = box(*bbox1)
        box2 = box(*bbox2)
        # Calculate the intersection of the two boxes
        intersection = box1.intersection(box2)
        # Return the area of the intersection
        return intersection.area
    except Exception as e:
        return f"Error in  intersected_area {str(e)}"
 
def utm_finder(epsg,bbox):
    """
    Find the UTM EPSG code for the given CRS and bounding box.
    """
    try:
        """
        Find UTM epsg
        raster: input raster path
        Returns:
        UTM EPSG code of the input raster
        """
        bbox_wgs84 = rasterio.warp.transform_bounds(epsg,'EPSG:4326', *bbox)
        utm_crs_list = query_utm_crs_info(     
            datum_name='WGS 84',
            area_of_interest= AreaOfInterest(
            west_lon_degree=bbox_wgs84[0],
            south_lat_degree=bbox_wgs84[1],
            east_lon_degree=bbox_wgs84[2],
            north_lat_degree=bbox_wgs84[3],),)
        utm_epsg_list=[]
        equal_intersection_epsg_list=[]
        largest_intersection_area = 0
        best_epsg = None
        
        for utm_crs in utm_crs_list:
            success, result=utm_epsg_bbox(utm_crs.code)
            if not success:
                return False, result
            epsg_bbox=result
            intersection_area = intersected_area(epsg_bbox,(bbox_wgs84[0],bbox_wgs84[1],bbox_wgs84[2],bbox_wgs84[3]))
            utm_epsg_list.append({"code":utm_crs.code,'name':utm_crs.name.replace("/",""), 'intersected_area':intersection_area})
            
            # if intersection_area > largest_intersection_area:
            #     largest_intersection_area = intersection_area
            #     best_epsg = utm_crs.code
                
            if intersection_area > largest_intersection_area:
                largest_intersection_area = intersection_area
                best_epsg = utm_crs.code
            elif intersection_area == largest_intersection_area:
                if utm_crs.code not in equal_intersection_epsg_list:
                    equal_intersection_epsg_list.append(utm_crs.code)
                if best_epsg not in equal_intersection_epsg_list:
                    equal_intersection_epsg_list.append(best_epsg)
                    
        if len(equal_intersection_epsg_list)>0:
            best_epsg=max(equal_intersection_epsg_list)
                    
        return True, best_epsg
    except Exception as e:
        return False, str(e)
    
def raster_data_cleaning(out_path):
    try:
    
        projected_raster = rasterio.open(out_path)
        transform = projected_raster.transform

        x_resolution = transform.a
        y_resolution = abs(transform.e)
        projected_data = projected_raster.read(1)
        rows, cols = projected_data.shape
        
        col_off_left = 0
        col_off_right = 0
        
        row_off_top = 0
        row_off_bottom = 0
        
        for col in range(cols):
            if (projected_data[:, col] == 0).all():
                col_off_left = col
            else:
                break
        
        for row in range(rows):
            if (projected_data[row, :] == 0).all():
                row_off_top = row
            else:
                break
        
        for col in range(cols-1, -1, -1):
            if (projected_data[:, col] == 0).all():
                col_off_right = col+1
            else:
                break
        
        for row in range(rows-1, -1, -1):
            if (projected_data[row, :] == 0).all():
                row_off_bottom = row+1
            else:
                break
        dst_width = col_off_right - col_off_left
        dst_height = row_off_bottom - row_off_top
        
        if dst_width>0 and dst_height>0:
            clipped_data = projected_data[row_off_top:row_off_bottom, col_off_left:col_off_right]
            x_origin = projected_raster.transform[2] + (col_off_left * x_resolution)
            y_origin = projected_raster.transform[5] - (row_off_top * y_resolution)
            new_transform = Affine(x_resolution, 0.0, x_origin, 0.0, -y_resolution, y_origin)
            dst_width = col_off_right - col_off_left
            dst_height = row_off_bottom - row_off_top
            
            kwargs = projected_raster.meta.copy()
            kwargs.update({
                'width': dst_width,
                'height': dst_height,
                'transform': new_transform,
                'compress': 'lzw'
            })
            with rasterio.open(out_path, 'w', **kwargs) as dst:
                dst.write(clipped_data, 1)
            
        projected_raster.close()
        return True, "success"
    except Exception as e:
        return False, str(e)

#Using rasterio
def rasterio_based_custom_raster_projection(file_path, out_path,resampling):
    print("rasterio based custom raster projection called")
    try:
        ds = rasterio.open(file_path)
        ds_crs_epsg = ds.crs.to_epsg()
        is_UTM_epsg = is_utm_epsg(ds_crs_epsg)
        
        dst_crs=ds.crs
        dst_utm_epsg=ds.crs.to_epsg()

        if not is_UTM_epsg:
            bbox  = ds.bounds
            success,result=utm_finder(ds_crs_epsg,bbox)
            if success:
                dst_utm_epsg=result
            else:
                return False, result
            src_crs=CRS.from_epsg(ds_crs_epsg) 
            dst_crs = CRS.from_epsg(dst_utm_epsg) 
            dst_transform, width, height = calculate_default_transform(
                src_crs.to_string(), dst_crs.to_string(), ds.width, ds.height, *ds.bounds
            )
            kwargs = ds.meta.copy()
            kwargs.update({
                        'crs': dst_crs.to_string(),
                        'transform': dst_transform,
                        'width': width,
                        'height': height})
            if resampling=="nearest":
                resampling_method=Resampling.nearest
            elif resampling=="bilinear":
                resampling_method=Resampling.bilinear
            elif resampling=="cubic":
                resampling_method=Resampling.cubic
            else:
                return False, "Invalid resampling method for rasterio basedcustom raster projection"
            print("applying resamplibg method-----------------------",resampling_method)
            with rasterio.open(out_path, 'w', **kwargs) as dst:
                for i in range(1, ds.count + 1):
                    reproject(
                        source=rasterio.band(ds, i),
                        destination=rasterio.band(dst, i),
                        src_transform=ds.transform,
                        src_crs=src_crs.to_string(),
                        dst_transform=dst_transform,
                        dst_crs=dst_crs.to_string(),
                        # resampling=Resampling.nearest
                        resampling=resampling_method
                        )
        ds.close()
        data_cleaning_success, data_cleaning_response =raster_data_cleaning(out_path)
        if data_cleaning_success==False:
            return False, data_cleaning_response
        return True, {"dst_crs":dst_crs,"dst_utm_epsg":dst_utm_epsg}
    except Exception as e:
        return False, str(e)
    
def custom_raster_projection(file_path, out_path, resampling="nearest"):
    '''
    resampling methods: nearest, bilinear, cubic
    '''
    print("gdal based custom raster projection called")
    try:
        gdal.SetConfigOption('GTIFF_HONOUR_NEGATIVE_SCALEY', 'YES')
        gdal.UseExceptions()
        ds = gdal.Open(file_path, gdal.GA_Update)
        # ds = gdal.OpenShared(file_path,gdal.GA_Update)  #Open a new image for updating
        gt = ds.GetGeoTransform()
        if not gt[5]<0:
            print("y resolution is not negative")
            updated_gt = (gt[0], gt[1], gt[2], gt[3], gt[4], -abs(gt[5]))
            ds.SetGeoTransform(updated_gt)
            ds.FlushCache()
        if ds is None:
            raise RuntimeError(f"Unable to open dataset {file_path} for updating")
        
        ds_crs_wkt = ds.GetProjection()
        ds_crs_epsg = int(osr.SpatialReference(wkt=ds_crs_wkt).GetAttrValue("AUTHORITY", 1))
        is_UTM_epsg = is_utm_epsg(ds_crs_epsg)
        
        dst_crs=ds_crs_wkt
        dst_utm_epsg=ds_crs_epsg
        ds = gdal.Open(file_path)
        if not is_UTM_epsg:
            bbox = (gt[0], gt[3] + gt[5] * ds.RasterYSize, gt[0] + gt[1] * ds.RasterXSize, gt[3])
            success, dst_utm_epsg = utm_finder(ds_crs_wkt, bbox)
            if not success:
                return False, str(dst_utm_epsg)
            dst_crs = osr.SpatialReference()
            dst_crs.ImportFromEPSG(int(dst_utm_epsg))
            dst_crs_wkt = dst_crs.ExportToWkt()
            
            if resampling=="nearest":
                resampling_method=gdal.GRA_NearestNeighbour
            elif resampling=="bilinear":
                resampling_method=gdal.GRA_Bilinear
            elif resampling=="cubic":
                resampling_method=gdal.GRA_Cubic
            else:
                return False, "Invalid resampling method for custom raster projection"
            print("applying resamplibg method-----------------------",resampling_method)
            gdal.Warp(out_path, ds, dstSRS=dst_crs_wkt, xRes=gt[1], yRes=gt[5], resampleAlg=resampling_method) #dstNodata=0,
            data_cleaning_success, data_cleaning_response =raster_data_cleaning(out_path)
            if data_cleaning_success==False:
                return False, data_cleaning_response
        return True, {"dst_crs":dst_crs,"dst_utm_epsg":dst_utm_epsg}
    except Exception as e:
        proj_success, proj_result=rasterio_based_custom_raster_projection(file_path,file_path,resampling)
        return proj_success, proj_result