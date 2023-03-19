import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.warp import transform

import os
from dateutil import parser 
from ckanext.cloudstorage.ckan_dataset import CkanDataset

# with rasterio.open('/mnt/temp/soil_moister/SMY2003DECA01.tif') as src:
#     print(src.width, src.height)
#     print(src.crs)
#     if src.crs == CRS.from_epsg(4326):
#         start_position = src.transform * (0,0)
#         end_position = src.transform * (src.width, src.height)
#         print(start_position[0], end_position[1])
#     else:
#         new_crs = CRS.from_epsg(4326)
#         start_position = transform(src.crs, new_crs, 
#                     xs=[0], ys=[0])
#         end_position =transform(src.crs, new_crs, 
#                     xs=[src.width], ys=[src.height])
#     print(src.transform)
#     print(src.count)
#     print(src.indexes)
#     start_position = src.transform * (0,0)
#     end_position = src.transform * (src.width, src.height)

class GEOTiffMetadata:
    # def getIndexes(self, ifile):
    #     return 

    def getInfo(self,ifile):

        crs=None
 
        with rasterio.open(ifile) as src:
            if src.crs == CRS.from_epsg(4326):
                start_position = src.transform * (0,0)
                end_position = src.transform * (src.width, src.height)
                print(start_position, end_position)
            else:
                new_crs = CRS.from_epsg(4326)
                start_position = transform(src.crs, new_crs, 
                            xs=[0], ys=[0])
                end_position =transform(src.crs, new_crs, 
                            xs=[src.width], ys=[src.height])

            # print(src.width, src.height)
            # print(src.crs)
            # print(src.transform)
            # print(src.count)
            # print(src.indexes)
            try:
                file_stats = os.stat(ifile)
                file_size = int(file_stats.st_size)
            except:
                file_size = 0
            count = src.count
            crs = src.crs
            
        return {'count':count,'lat_start':start_position[1],'lat_end':end_position[1],'lon_start':start_position[0], 'lon_end':end_position[0],'crs':crs.to_string(),'file_size':file_size} 

def import_tiffinfo_to_package(package_id=None,resource_id=None,file=None):

    latlon_list=[]
    geotiff_md = GEOTiffMetadata()
    info = geotiff_md.getInfo(file)
    latlon_info = [[info['lon_start'],info['lat_start']],[info['lon_start'],info['lat_end']],[info['lon_end'],info['lat_end']],[info['lon_end'],info['lat_start']],[info['lon_start'],info['lat_start']]]
    latlon_list.append(latlon_info)
    spatial_info = {"type":"Polygon","coordinates":latlon_list}
    par_info=[{'key':'bound_counts','value':info['count']},{'key':'crs','value':info['crs']}]
    time_span = {'time_step':0, 'start_time':'0000-00-00 00:00:00', 'end_time':'0000-00-00 00:00:00'}
    ckandataset = CkanDataset(package_id=package_id)
    ckandataset.lcPatchPackage(package_id,spatial_info,time_span,par_info)
    # ckandataset.lcPatchResource(resource_id,spatial_info,time_span,par_info)
# latlon_info is {'type': 'Polygon', 'coordinates': [[[18.3, 18.3], [18.3, 53.5], [53.5, 53.5], [53.5, 18.3], [18.3, 18.3]]]}
#  updated time info is {'time_step': 47, 'start_time': '2005-10-21T00:00:00', 'end_time': '2005-10-21T23:00:00'}
#  par info is [{'key': 'par1_name ', 'value': 'ozone'}, {'key': 'par1_units ', 'value': '"ppb"'}, {'key': 'par1_datatype ', 'value': 'F32'}]


if __name__=='__main__':
    geotiff_md = GEOTiffMetadata()
    # url = '/mnt/temp/ERA5land_hourly'
    # 文件夹不要以/结尾
    url = '/srv/app/src/ckanext-cloudstorage/testdata/tiff' 
    name = os.path.basename(url).replace('.','_').lower()

    # ckandataset = CkanDataset(name=name,title=name,private=False,author='liudi', author_email='liudi@rcees.ac.cn')
    # package_id = ckandataset.CreatePackage()
    package_name= '256f5ba9-7acb-4b6a-af6f-b7a3c6e35842'
    package_id= CkanDataset.GetPackageById(package_name)
    # print(package_id)
    ckandataset = CkanDataset(package_id=package_id)

    if package_id is not None:
        
        update_latlon_info = {}
        update_time_info ={}
        latlon_list=[]
        for f in os.listdir(url):
            real_url = os.path.join(url, f)
            if os.path.isfile(real_url):
                info = geotiff_md.getInfo(real_url)
                # GeoTiff数据集的tiff文件可能覆盖的空间范围不一致，因此spatial字段应该是一个数组
                latlon_info = [[info['lon_start'],info['lat_start']],[info['lon_start'],info['lat_end']],[info['lon_end'],info['lat_end']],[info['lon_end'],info['lat_start']],[info['lon_start'],info['lat_start']]]
                latlon_list.append(latlon_info)
                
                fsize=info['file_size']
                name = f.replace('.','_').lower()
                
             
                ckandataset.CreateResource(package_name,name, fsize,real_url,"GeoTiff")
        
        spatial_info = {"type":"Polygon","coordinates":latlon_list}
        par_info=[{'key':'bound_counts','value':info['count']},{'key':'crs','value':info['crs']}]
        time_span = {'time_step':0, 'start_time':'0000-00-00 00:00:00', 'end_time':'0000-00-00 00:00:00'}
        
        ckandataset.PatchPackage(package_name,spatial_info,time_span,par_info)
 