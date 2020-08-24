# In[1]:
import RiskChanges
EAR_poly="D:\\SDSS\\Sample data\\Jamoats_data.shp"
EAR_poin="D:\\SDSS\\Sample data\\point_test.shp"
EAR_line="D:\\SDSS\\Sample data\\line_test.shp"
Haz="D:\\SDSS\\Sample data\\flood.tif"

#Reclassify 
# In[2]
classif={'low':[0,2],'medium':[1,3],'high':[2,10]}
Exposure.standardizeHaz(in_image="D:\\SDSS\\Sample data\\flood.tif",out_image="classAshok",out_dir="D:\\SDSS\\Sample data",classification=classif)
# In[3]:
poly_out=Exposure.computeExposure(input_zone=EAR_poly, input_value_raster=Haz,earsource='shp')
# In[4]:
poi_out=Exposure.computeExposure(input_zone=EAR_poin, input_value_raster=Haz,earsource='shp')
# In[5]:
lin_out=Exposure.computeExposure(input_zone=EAR_line, input_value_raster=Haz,earsource='shp')


#classif={'low':[0,2],'medium':[1,3],'high':[2,10]}
#reclassify(in_image="D:\\SDSS\\Sample data\\flood.tif",out_image="classAshok",out_dir="D:\\SDSS\\Sample data",classification=classif)




#curenull(inputHaz="D:\\SDSS\\Sample data\\flood.tif",replace=7,outputHaz="NonullAsgok",outdir="D:\\SDSS\\Sample data")


# In[13]:


#changeProj(inhaz="D:\\SDSS\\Sample data\\flood.tif",outhaz="Ashok",outdir="D:\\SDSS\\Sample data",epsg=4326)

