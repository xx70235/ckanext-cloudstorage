from ckanapi import RemoteCKAN, NotAuthorized
import json
import xarray as xr 
import os
import logging
from ckanapi import LocalCKAN

log = logging.getLogger(__name__)
ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'
url = 'http://127.0.0.1:5000'
apikey = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJkdWRnRUoycWhaQnJEZmplWVp2Y05jVTR2a1RBQy1UYlFUR2s5RjVCQjFNbFlzYXdzNU9KcS1pRnRjSmEtVnV2VEpBekpZNEdUNnU0V1lTTiIsImlhdCI6MTY3ODg3NjE2MH0.wKolAvyvHxBuFwghx3-WjFZN_b3cI3nCmvsgT6X5lIA'

class CkanDataset:

    def __init__(self,name=None,title=None,private=False,author=None,author_email=None,notes=None,groups=[{'name':'eesg'}],owner_org='rcees',package_id = None ):
        # self.fpath = fpath
        # self.fsize = fsize
        self.name = name
        self.title =title
        self.notes = notes
        self.private = private
        self.author = author
        self.author_email = author_email   
        self.groups = groups
        self.owner_org = owner_org
        self.package_id = package_id
        # self.spatial = spatial
        # self.timespan = timespan
        # self.pars = pars
    def CreatePackage(self):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            try:
                pkg = ckan.action.package_create(name=self.name, title=self.title,private=self.private,author=self.author,author_email=self.author_email,maintainer=self.author,maintainer_email=self.author_email,groups=self.groups, owner_org=self.owner_org)
                print("dataset {} created".format(pkg['id']))
                package_id = pkg['id']
                return package_id
            except NotAuthorized:
                    print('denied')
                    return None
    
    def PatchPackage(self, package_id, spatial,timespan,pars ):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            extras=[
                {"key":"spatial","value":json.dumps(spatial)},
                {"key":"start_time","value":json.dumps(timespan['start_time'])},
                {"key":"end_time","value":json.dumps(timespan['end_time'])},
                {"key":"time_step","value":json.dumps(timespan['time_step'])}           
            ]
            extras.extend(pars)
            print(extras)
            try:
                pkg = ckan.action.package_patch(id=package_id,extras=extras)
                print("dataset {} updated".format(pkg['id']))
            
            except NotAuthorized:
                print('denied')
    def lcPatchPackage(self, package_id, spatial,timespan,pars):
        extras=[
                    {"key":"spatial","value":json.dumps(spatial)},
                    {"key":"start_time","value":json.dumps(timespan['start_time'])},
                    {"key":"end_time","value":json.dumps(timespan['end_time'])},
                    {"key":"time_step","value":json.dumps(timespan['time_step'])}           
                ]
        extras.extend(pars)
      
        log.info("extra is {}".format(extras))
        lc = LocalCKAN()
        lc.action.package_patch(id=package_id,extras=extras)
      


    def lcPatchResource(self, resource_id, spatial,timespan,pars):
        extras=[
                        {"key":"spatial","value":json.dumps(spatial)},
                        {"key":"start_time","value":json.dumps(timespan['start_time'])},
                        {"key":"end_time","value":json.dumps(timespan['end_time'])},
                        {"key":"time_step","value":json.dumps(timespan['time_step'])}           
                ]
        extras.extend(pars)
        lc = LocalCKAN()
        lc.action.resource_patch(id=resource_id,extras=extras)

    def PatchResource(self,resource_id, size):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            try:
                log.info("update file size")
                info={"id":resource_id,"size":size}
             
                res = ckan.action.resource_patch(id=resource_id,size=size)
                log.info("updated res is").format(res)

                # print("resource {} updated".format(pkg['id']))
            
            except NotAuthorized:
                print('denied')
               

    def CreateResource(self,package_id,name, fsize,fpath,file_type="NetCDF"):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            name = name.replace('.','_').lower()
            try:
                res = ckan.action.resource_create(package_id=package_id,url=name,format=file_type,name=name,size=fsize,upload=open(fpath, 'rb'))
                # print("Resource {} created".format(res['id']))

            except NotAuthorized:
                print('denied')


    @classmethod
    def GetPackageByName(cls, package_name):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            try:
                # res = ckan.action.package_show(package_name)
                res = ckan.call_action('package_show',{'id':package_name})
                return res
            except NotAuthorized:
                print('denied')    
                return None
                
    @classmethod
    def GetPackageById(cls, id):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            try:
                # res = ckan.action.package_show(package_name)
                res = ckan.call_action('package_show',{'id':id})
                return res
            except NotAuthorized:
                print('denied')    
                return None
            
    @classmethod
    def UploadResource(cls,package_id,name, fsize,fpath,file_type="NetCDF"):
        with RemoteCKAN(url, apikey=apikey, user_agent=ua) as ckan:
            name = name.replace('.','_').lower()
            try:
                res = ckan.action.resource_create(package_id=package_id,url=name,format=file_type,name=name,size=fsize,upload=open(fpath, 'rb'))
                # print("Resource {} created".format(res['id']))

            except NotAuthorized:
                print('denied')
 

if __name__=='__main__':
    package_name= 'gimmiss3g'
    res= CkanDataset.GetPackageByName(package_name)
    print(res['id'])
    # nc_file = xr.open_dataset('./test_data/ndvi3g_geo_v1_1981_0712.nc4')
    # bT = nc_file['ndvi']
    # bT = bT.rio.set_spatial_dims(x_dim='lon', y_dim='lat')
    # bT.rio.write_crs("epsg:4326", inplace=True)
    # bT.rio.to_raster(r"ndvi3g_geo_v1_1981_0712_raster.tiff")
    file_path = '/mnt/cdo/test_data/ndvi3g_geo_v1_1981_0712_raster.tiff'
    fsize = int(os.stat(file_path).st_size)



            
    CkanDataset.UploadResource(res['id'],'ndvi3g_geo_v1_1981_0712_raster',fsize,file_path,file_type="GeoTiff")


