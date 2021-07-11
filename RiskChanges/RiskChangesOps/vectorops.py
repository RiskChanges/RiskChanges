from rasterio.windows import Window
from rasterio.windows import from_bounds
from rasterio.mask import mask
import rasterio

def cehckprojection(ear,haz):
    ear_epsg=ear.crs.to_epsg()
    haz_epsg=haz.crs.to_epsg()
    if ear_epsg==haz_epsg:
        return True
    else:
        return False

