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
        
        #if it is the first value
        if i==0:
            continue
        # if it is the last value, need to assign the max class for all result
        elif threshold == thresholds[-1]:
            intensity_data_classified[(
                intensity_data >= thresholds[-1])] = i
        else:
            intensity_data_classified[(
                (intensity_data < threshold) & (intensity_data >= prev))] = i
        prev = threshold
        
    with rasterio.Env():
        profile = input_image.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(intensity_data_classified, 1)
        # dst = None
    # input_image = None
    input_image.close()
    
def refactor(in_image, out_image, base, threshold):
    src = rasterio.open(in_image)
    src_data = src.read(1)
    has_nodata = np.isnan(src_data).any()
    if has_nodata:
        src_data = np.nan_to_num(src_data, nan=0.0)
    nodata = src.nodata #this will handle value with -999
    src_data[src_data==nodata]=0.0
    src_data[src_data < base] = 0.0
    src_data[src_data > threshold] = 0.0
    with rasterio.Env():
        profile = src.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(src_data, 1)
    src.close()


def ClassifyHazard(hazard_file, base, stepsize, threshold,is_reclassification_required=False):
    infile = hazard_file
    file_name, file_extension = os.path.splitext(infile)
    outfile = hazard_file.replace(file_extension, f"_reclassified{file_extension}")
    if is_reclassification_required or not os.path.isfile(outfile):
        reclassify(infile, outfile, base, stepsize, threshold)
    # if os.path.isfile(outfile):
    #     pass
    # else:
    return outfile

def RefactorClassifiedHazard(hazard_file, base, threshold,is_reclassification_required=False):
    file_name, file_extension = os.path.splitext(hazard_file)
    outfile = hazard_file.replace(file_extension, f"_reclassified{file_extension}")
    if is_reclassification_required or not os.path.isfile(outfile):
        refactor(hazard_file, outfile, base, threshold)
    # elif not os.path.isfile(outfile):
    #     refactor(hazard_file, outfile, base, threshold)
    # if os.path.isfile(outfile):
    #     pass
    # else:
    return outfile



def readhaz(connstr, hazid, haz_file):
    hazard_metadata = readmeta.hazmeta(connstr, hazid)
    base = hazard_metadata.base_val[0] or 0
    step_size = hazard_metadata.interval_val[0] or 1
    hazfile = hazard_metadata.file[0]
    threshold = hazard_metadata.threshold_val[0] or hazard_metadata.raster_max_value[0]
    intensity_type = hazard_metadata.intensity[0]
    unit=hazard_metadata.unit[0]
    is_reclassification_required=hazard_metadata.is_reclassification_required[0]
    
    if haz_file:
        hazfile = haz_file

    if intensity_type == 'Susceptibility' and unit== "classes":
        # outfile = hazfile
        outfile = RefactorClassifiedHazard(hazfile, base, threshold,is_reclassification_required)
    else:
        outfile = ClassifyHazard(hazfile, base, step_size, threshold,is_reclassification_required)
        
    src = rasterio.open(outfile)
    return src

# def read_raster_ear(connstr, ear_id, ear_file):
#     ear_metadata = readmeta.earmeta(connstr, ear_id)
#     src = rasterio.open(outfile)
#     return src