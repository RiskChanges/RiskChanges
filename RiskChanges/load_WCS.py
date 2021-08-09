
from owslib.wcs import WebCoverageService


def loadWCS(folder, out_name, WCS_URL, Version, layerName, bbox, srid):
    folder = folder
    urls = WCS_URL
    version = Version
    bbox = bbox
    input_value_raster = layerName
    crs_num = 'urn:ogc:def:crs:EPSG::'+str(srid)

    if urls == 'None':
        return("Please provide WCS parameters")

    wcs = WebCoverageService(urls, version)
    # print(list(wcs.contents))

    #
    # print([op.name for op in wcs.operations])

    cvg = wcs.contents[input_value_raster]
    if bbox == 'None':
        bbox = cvg.boundingBoxWGS84

    response = wcs.getCoverage(identifier=input_value_raster, bbox=bbox,
                               format='GEOTIFF_FLOAT32', crs='urn:ogc:def:crs:EPSG::28992', resx=0.5, resy=0.5)
    temp_raster = folder+'//'+out_name+'.tif'
    with open(temp_raster, 'wb') as file:
        file.write(response.read())
