from . import readmeta 
import rasterio
import numpy as np
import os
def reclassify(in_image,out_image,base,stepsize):
    input_image=rasterio.open(in_image)
    intensity_data=input_image.read(1)
    maxval=np.max(intensity_data)
    #print(maxval)
    prev=base
    i=1
    thresholds=np.arange(start=base,stop=maxval+stepsize,step=stepsize)[1:].tolist()
    #print(thresholds)
    intensity_data[intensity_data<base]=input_image.get_nodatavals()[0]
    intensity_data_classified=np.copy(intensity_data)
    for threshold in thresholds:
        #mean=intensity_data[((intensity_data<threshold) & (intensity_data>=prev))].mean()
        intensity_data_classified[((intensity_data<threshold) & (intensity_data>=prev))]=i
        mean=intensity_data_classified[((intensity_data<threshold) & (intensity_data>=prev))].mean()
        #print(mean)
        i+=1
        prev=threshold

    with rasterio.Env():
        profile = input_image.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(intensity_data_classified, 1)
        dst=None
    input_image=None


def ClassifyHazard(hazard_file,base,stepsize):
    infile=hazard_file
    outfile=hazard_file.replace(".tif","_reclassified.tif")
    if  os.path.isfile(outfile):
        pass
    else:
        reclassify(infile,outfile,base,stepsize)
    return outfile

def readhaz(connstr,hazid, haz_file):
    hazard_metadata=readmeta.hazmeta(connstr,hazid)
    base=hazard_metadata.base_val[0]
    step_size=hazard_metadata.interval_val[0]
    hazfile=hazard_metadata.file[0]
    intensity_type=hazard_metadata.intensity[0]
    if haz_file:
        hazfile = haz_file
        
    if intensity_type=='Susceptibility':
        outfile=hazfile
    else:
        outfile=ClassifyHazard(hazfile,base,step_size)
        
    src=rasterio.open(outfile)
    return src


