from . import readmeta
import rasterio
import numpy as np
import os

def reclassify(in_image, out_image, base, stepsize, maxval):
    input_image = rasterio.open(in_image)
    intensity_data = input_image.read(1)
    has_nodata = np.isnan(intensity_data).any()
    if has_nodata:
        intensity_data = np.nan_to_num(intensity_data, nan=0.0)
    nodata = input_image.nodata #this will handle value with -999
    intensity_data[intensity_data==nodata]=0.0
    prev = base
    thresholds = np.arange(start=base, stop=maxval+1, step=stepsize).tolist()
    intensity_data[intensity_data < base] = 0.0
    # intensity_data[intensity_data < base] = input_image.nodata
    intensity_data_classified = np.copy(intensity_data)
    for i, threshold in enumerate(thresholds):
        #mean=intensity_data[((intensity_data<threshold) & (intensity_data>=prev))].mean()
        intensity_data_classified[(
            (intensity_data < threshold) & (intensity_data >= prev))] = i
        prev = threshold
        # if it is the last value, need to assign the max class for all result
        if threshold == thresholds[-1]:
            intensity_data_classified[(
                intensity_data >= thresholds[-1])] = i
    with rasterio.Env():
        profile = input_image.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(intensity_data_classified, 1)
        dst = None
    input_image = None


def ClassifyHazard(hazard_file, base, stepsize, threshold):
    infile = hazard_file
    outfile = hazard_file.replace(".tif", "_reclassified.tif")
    if os.path.isfile(outfile):
        pass
    else:
        reclassify(infile, outfile, base, stepsize, threshold)
    return outfile


def readhaz(connstr, hazid, haz_file):
    hazard_metadata = readmeta.hazmeta(connstr, hazid)
    base = hazard_metadata.base_val[0] or 0
    step_size = hazard_metadata.interval_val[0] or 1
    hazfile = hazard_metadata.file[0]
    threshold = hazard_metadata.threshold_val[0] or hazard_metadata.raster_max_value[0]
    intensity_type = hazard_metadata.intensity[0]
    if haz_file:
        hazfile = haz_file

    if intensity_type == 'Susceptibility':
        outfile = hazfile
    else:
        outfile = ClassifyHazard(hazfile, base, step_size, threshold)

    src = rasterio.open(outfile)
    return src
