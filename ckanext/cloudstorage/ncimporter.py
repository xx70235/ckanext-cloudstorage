'''通过cdo及其python wrapper调出时间范围、空间范围、变量字段，调用ckan API存入数据库'''
from cdo import Cdo,CDOException,CdoTempfileStore
import os

from dateutil import parser 
from ckanext.cloudstorage.ckan_dataset import CkanDataset
import tempfile

import logging
log = logging.getLogger(__name__)
class NCMetaData:

    '''
    get spatial info
    '''
    def getLatlon(self, ifile):
        cdo = Cdo()
        log.info("input file is {}".format(ifile))
        s = cdo.griddes(input =ifile)
        haslat = False
        latRowName = ''
        haslon = False
        lonRowName = ''
        
        for i in s:
            if 'lon' in i:
                haslon = True
                lonRowName = i.split(' ')[0]
                # print(lonRowName)
            if 'lat' in i:
                haslat = True
                latRowName = i.split(' ')[0]
                # print(latRowName)

        for i in s:
            if haslon and len(lonRowName)>0:
                if i.startswith(lonRowName[0]+'first'):
                    lon_start=float(i.split('=')[1])
                    # print(lon_start)
                if i.startswith(lonRowName[0]+'inc'):
                    lon_inc=float(i.split('=')[1])
                    # print(lon_inc)
                if i.startswith(lonRowName[0]+'size'):
                    lon_size=float(i.split('=')[1])
                    # print(lon_size)
        if lon_size>0 and lon_inc !=0:
            lon_end = lon_start+(lon_size-1)*(lon_inc)
            # print(lon_end)

        for i in s:
            if haslat and len(latRowName)>0:
                if i.startswith(latRowName[0]+'first'):
                    lat_start=float(i.split('=')[1])
                    # print(lat_start)
                if i.startswith(latRowName[0]+'inc'):
                    lat_inc=float(i.split('=')[1])
                    # print(lat_inc)
                if i.startswith(latRowName[0]+'size'):
                    lat_size=float(i.split('=')[1])
                    # print(lat_size)
        if lat_size>0 and lat_inc !=0:
            lat_end = lat_start+(lat_size-1)*(lat_inc)

        return {
        "type":"Polygon",
        "coordinates":[[[lon_start,lat_start],[lon_start,lat_end],[lon_end,lat_end],[lon_end,lat_start],[lon_start,lat_start]]]}

    '''
    get time span
    '''
    def getTimespan(self, ifile):
        cdo = Cdo()
        s = cdo.showtimestamp(input =ifile)
        print(len(s))
        print(len(s[0]))
        if len(s)>=1 and len(s[0])>0:
            ts = s[0].split(' ')
            time_step = len(ts)
            if time_step>0:
                start_time = ts[0]
                end_time = ts[-1]
                return {'time_step':time_step, 'start_time':start_time, 'end_time':end_time}
            else:
                return {'time_step':time_step, 'start_time':'0000-00-00 00:00:00', 'end_time':'0000-00-00 00:00:00'}
        else:
            return {'time_step':0, 'start_time':'0000-00-00 00:00:00', 'end_time':'0000-00-00 00:00:00'}
        # s = cdo.sinfo(input=ifile)
        # print(s)

    '''
    get the parameters info
    '''
    def getPars(self, ifile):
        cdo = Cdo()
        s = cdo.partab(input =ifile)
        # print(s)
        i = 0
        pars = []
        
        while(i<len(s)):
            if s[i]=='&parameter':
                par_info = {}
                i=i+1
            if s[i]!='/':
                par_element = s[i].split('=')
                par_info[par_element[0]] = par_element[1]
                i=i+1
            if s[i] == '/':
                pars.append(par_info)
                i=i+1
        par_list=[]
        # extras 表示各种字段，以key value形式
        for idx, par in enumerate(pars,1):
            for key in par.keys():
                par_item = {"key":"par"+str(idx)+"_"+key, "value":par[key].strip()}
                par_list.append(par_item)
        return par_list


    def getFileSize(self, ifile):
        file_stats = os.stat(ifile)
        return int(file_stats.st_size)
            

def import_ncinfo_to_package(package_id=None,resource_id=None,file=None):

    latlon_list=[]
    update_latlon_info = {}
    update_time_info ={}
    ncMetaData = NCMetaData()
    with tempfile.NamedTemporaryFile(prefix='tmp_', suffix='.nc') as tmp_file:
        # 将 _io.FileIO 对象的内容写入临时文件中
        tmp_file.write(file.read())
        tmp_file.flush()
        filepath = tmp_file.name
        latlon_info = ncMetaData.getLatlon(filepath)
        log.info("latlon_info is {}".format(latlon_info))

        update_latlon_info = latlon_info

        time_info = ncMetaData.getTimespan(filepath)
        # log.info("time info is {}".format(time_info))

        try:
            if len(update_time_info)<=0:
                update_time_info = time_info
            if parser.parse(time_info['start_time'])<parser.parse(update_time_info['start_time']):
                update_time_info['start_time'] = time_info['start_time']
                log.info(update_time_info['start_time'])
            if parser.parse(time_info['end_time'])>parser.parse(update_time_info['end_time']):
                update_time_info['end_time'] = time_info['end_time']
                log.info(update_time_info['end_time'])
        except parser.ParserError:
            pass
        log.info("updated time info is {}".format(update_time_info))

        par_info = ncMetaData.getPars(filepath)
        log.info("par info is {}".format(par_info))
        log.info("package_id is {}".format(package_id))


    ckandataset = CkanDataset(package_id=package_id)
    # update_time_info = {'time_step':0, 'start_time':'0000-00-00 00:00:00', 'end_time':'0000-00-00 00:00:00'}
    ckandataset.lcPatchPackage(package_id,update_latlon_info,update_time_info,par_info)
    

if __name__=='__main__':
    ncMetaData = NCMetaData()
    # url = '/mnt/temp/ERA5land_hourly'
    # 文件夹不要以/结尾
    url = '/srv/app/src/ckanext-cloudstorage/testdata' 
    name = os.path.basename(url).replace('.','_').lower()

    # ckandataset = CkanDataset(name=name,title=name,private=False,author='liudi', author_email='liudi@rcees.ac.cn')
    # package_id = ckandataset.CreatePackage()
    # get package_id
    package_id= '256f5ba9-7acb-4b6a-af6f-b7a3c6e35842'
    # package_id= CkanDataset.GetPackageById(package_name)
    ckandataset = CkanDataset(package_id=package_id)

    if package_id is not None:
        
        update_latlon_info = {}
        update_time_info ={}
        for f in os.listdir(url):
            real_url = os.path.join(url, f)
            if os.path.isfile(real_url):
                latlon_info = ncMetaData.getLatlon(real_url)
                if len(update_latlon_info)<=0 or update_latlon_info["coordinates"] != latlon_info["coordinates"]:
                    update_latlon_info = latlon_info

                time_info = ncMetaData.getTimespan(real_url)
                print(time_info)
                try:
                    if len(update_time_info)<=0:
                        update_time_info = time_info
                    if parser.parse(time_info['start_time'])<parser.parse(update_time_info['start_time']):
                        update_time_info['start_time'] = time_info['start_time']
                        print(update_time_info['start_time'])
                    if parser.parse(time_info['end_time'])>parser.parse(update_time_info['end_time']):
                        update_time_info['end_time'] = time_info['end_time']
                        print(update_time_info['end_time'])
                except parser.ParserError:
                    pass
                par_info = ncMetaData.getPars(real_url)
                # fsize=ncMetaData.getFileSize(real_url)
                name = f.replace('.','_').lower()
                
             
                # ckandataset.CreateResource(package_id,name, fsize,real_url)
        
        ckandataset.lcPatchPackage(package_id,update_latlon_info,update_time_info,par_info)
 
    



