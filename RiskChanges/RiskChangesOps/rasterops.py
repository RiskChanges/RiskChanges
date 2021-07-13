from rasterio.windows import Window
from rasterio.windows import from_bounds
from rasterio.mask import mask
import rasterio
def cropraster(raster,geometry):
    maska,transform=rasterio.mask.mask(raster, geometry, crop=True,nodata=0,all_touched=True)
    return maska,transform