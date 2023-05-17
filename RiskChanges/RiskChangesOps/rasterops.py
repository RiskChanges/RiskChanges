from rasterio.windows import Window
from rasterio.windows import from_bounds
from rasterio.mask import mask
import rasterio
def cropraster(raster,geometry):
    try:
        maska,transform=rasterio.mask.mask(raster, geometry, crop=True,nodata=0,all_touched=True,filled=True)
        image=rasterio.features.rasterize(geometry, out_shape=maska.shape[1:],fill=0,all_touched=True,transform=transform)
        len_ras=image.sum()
        return maska,transform,len_ras
    except Exception as e:
        print(f"error from crop raster: {str(e)}")
        return None