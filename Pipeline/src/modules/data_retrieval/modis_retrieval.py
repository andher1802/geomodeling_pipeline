import os
import sys
import argparse

import datetime
import time
import re
import numpy as np
import pandas as pd
import ee
import geopandas as gpd
import rasterio
import rioxarray
import shapely
import requests
import lxml.html
import urllib

from dateutil.relativedelta import *
from osgeo import ogr, osr, gdal
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
from subprocess import Popen
from getpass import getpass
from netrc import netrc

from data_processing.knmi_interpolation import Knmi_Interpolator


class ModisRetrieval(Knmi_Interpolator):
    vars_names = []
    bounds = []

    def __init__(self, parent_folder='./data'):
        Knmi_Interpolator.__init__(self, parent_folder)
        super().__init__(parent_folder)
        try:
            ee.Initialize()
            print('Google Earth Engine has initialized successfully!')
        except ee.EEException as e:
            print('Google Earth Engine has failed to initialize!')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
   
    def get_crs_image_collection(self, image_collection):
        return ee.Image(image_collection.first()).projection().crs()
    
    def set_bounds(self, bounds):
        self.bounds = bounds
    
    def set_vars_names(self, vars_names):
        self.vars_names = vars_names
    
    def get_ee_data(self, dataset_name):
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        start_date_filter = self.start_date.strftime('%Y-%m-%d')
        end_date_filter = self.end_date.strftime('%Y-%m-%d')
        self.set_interpolation_boundaries()
        try:
            data_ee = ee.ImageCollection(dataset_name).filterDate(start_date_filter, end_date_filter).select(self.vars_names)
            mosaic = data_ee.mean()
            proj = ee.Projection(self.interpolation_output_crs);
            ext_factor = 1
            aoi = ee.Geometry.Polygon(
                  [[[self.boundaries[1] * ext_factor, self.boundaries[2] * ext_factor],
                    [self.boundaries[1] * ext_factor, self.boundaries[0] * ext_factor],
                    [self.boundaries[3] * ext_factor, self.boundaries[0] * ext_factor],
                    [self.boundaries[3] * ext_factor, self.boundaries[2] * ext_factor]]], proj, False)
            reprojected = mosaic.reproject(proj, None, 1);
            band_arrs = reprojected.sampleRectangle(region=aoi).getInfo()
            return band_arrs
        except ee.EEException as e:
            print('error', e)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise    

class NASARetrieval(Knmi_Interpolator):
    fileList = None
    urs = None
    netrcDir = None
    vars_names = []
    tiles = None
    product = None
    
    def __init__(self, parent_folder='./data'):
        Knmi_Interpolator.__init__(self, parent_folder)
        super().__init__(parent_folder)
    
    def set_urs(self, urs):
        self.urs = urs
        
    def set_file_list(self, files):
        self.fileList = list(files)
    
    def set_netrcDir(self, netrc_name):
        self.netrcDir = os.path.expanduser(netrc_name)
    
    def set_vars_names(self, vars_names):
        self.vars_names = vars_names
        
    def set_type_data(self, type_data):
        self.type_data = type_data
    
    def set_tiles(self, tiles):
        self.tiles = tiles
    
    def set_product(self, product):
        self.product = product
    
    def download_list(self, nmonths):
        fileList_2 = []
        for i in range(1, nmonths + 1):
            today = self.start_date
            j = i - 1
            d = today.replace(day=1) + relativedelta(months=j)
            dt = d.strftime('%Y.%m.%d')
            url = "https://e4ftl01.cr.usgs.gov/{2}/{0}/{1}/".format(self.product, dt, self.type_data)
            f = requests.get(url)
            element_tree = lxml.html.fromstring(f.text) 
            url_ahref  = element_tree.xpath('//a/@href')
            arr = np.array(url_ahref)
            arr_f = []
            for element in arr:
                if self.tiles in element and element.endswith('.hdf'):
                    arr_f.append(True)
                else:
                    arr_f.append(False)
            arr_filterd = arr[arr_f]
            filename_hrz = arr_filterd[0]
            download_url = url + filename_hrz
            fileList_2.append(download_url)
        self.set_file_list(fileList_2)
        
    def derive_file_list(self):
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        start_date_filter = self.start_date.strftime('%Y/%m/%d')
        end_date_filter = self.end_date.strftime('%Y/%m/%d')
        range_dates = pd.date_range(start_date_filter, end_date_filter, freq="MS")
        diff_months = len(range_dates)
        self.download_list(diff_months)
        
    def set_up_credentials(self):
        prompts = [
            'Enter NASA Earthdata Login Username \n(or create an account at urs.earthdata.nasa.gov): ',
            'Enter NASA Earthdata Login Password: '
        ]
        try:
            self.set_netrcDir("~/.netrc")
            netrc(self.netrcDir).authenticators(self.urs)[0]

        except FileNotFoundError:
            homeDir = os.path.expanduser("~")
            Popen('touch {0}.netrc | chmod og-rw {0}.netrc | echo machine {1} >> {0}.netrc'.\
                  format(homeDir + os.sep, self.urs), shell=True)
            Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
            Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)

        except TypeError:
            homeDir = os.path.expanduser("~")
            Popen('echo machine {1} >> {0}.netrc'.format(homeDir + os.sep, self.urs), shell=True)
            Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
            Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)
        
        tries = 0
        while tries < 30:
            try:
                netrc(netrcDir).authenticators(urs)[2]
            except:
                time.sleep(2.0)
            tries += 1

    def download_files(self):
        for f in self.fileList:
            self.set_filename_output(f.split('/')[5]+'_'+self.product+'.hdf')
            with requests.get(f.strip(),\
                              verify=False,\
                              stream=True,\
                              auth=(netrc(self.netrcDir).authenticators(self.urs)[0],\
                                    netrc(self.netrcDir).authenticators(self.urs)[2])) as response:
                if response.status_code != 200:
                    print("{} not downloaded. Verify that your username and password are correct in {}".\
                          format(f.split('/')[-1].strip(), self.netrcDir))
                else:
                    response.raw.decode_content = True
                    content = response.raw
                    with open(self.output_filename, 'wb') as d:
                        while True:
                            chunk = content.read(16 * 1024)
                            if not chunk:
                                break
                            d.write(chunk)
                    print('Downloaded file: {}'.format(self.output_filename ))
    
    def array_to_tif(self, data_to_save):
        band_array = data_to_save['band_array']
        band_ds = data_to_save['template']
        band_path = data_to_save['name']
        band_array[band_array == -3000] = -32768
        scale_raster_values = 1.0000
        if data_to_save['layer'] == 'NVDI':
            scale_raster_values = 0.0001
        elif data_to_save['layer'] == 'EVI':
            scale_raster_values = 1.0000
        try:
            band_array = np.round(band_array*scale_raster_values, decimals=4)
            out_ds = gdal.GetDriverByName('GTiff').Create(band_path,
                                                      band_array.shape[0],
                                                      band_array.shape[1],
                                                      1,
                                                      gdal.GDT_Int16,
                                                      ['COMPRESS=LZW', 'TILED=YES'])
            out_ds.SetGeoTransform(band_ds.GetGeoTransform())
            out_ds.SetProjection(band_ds.GetProjection())
            out_ds.GetRasterBand(1).SetNoDataValue(-32768 * scale_raster_values)
            out_ds.GetRasterBand(1).WriteArray(band_array)
            print('file {0} created successfully'.format(band_path))
        except Exception as e: print(e)
    
    def __print_layers_subdatasets(self, subdatasets):
        for fname, name in subdatasets:
            print (name, "------", fname)
    
    def read_sub_datasets(self, filename_raster):
        filename_raster_no_folder = filename_raster.split('/')[-1]
        nasa_raster = gdal.Open(filename_raster)
        if nasa_raster is None:
            print ("Problem opening file!")
            return None
        else:
            subdatasets = nasa_raster.GetSubDatasets()
            self.__print_layers_subdatasets(subdatasets)
            if self.type_data == 'MOLT':
                file_template = 'HDF4_EOS:EOS_GRID:"%s":MOD_Grid_monthly_1km_VI:%s'
            elif self.type_data == 'MOTA':
                file_template = 'HDF4_EOS:EOS_GRID:"%s":MOD_Grid_BRDF:%s'
            for i, layer in enumerate ( self.vars_names ):
                data = {}
                this_file = file_template % ( filename_raster, layer )
                g = gdal.Open(this_file)
                if g is None:
                    raise IOError
                data['band_array'] = g.ReadAsArray().astype(np.int16)
                data['name'] = '.'.join(filename_raster_no_folder.split('.')[:-1])+'_'+layer+'.tiff'
                data['template'] = g
                data['layer'] = layer
                self.array_to_tif(data)
            return data        
                    
    def hdf_to_tiff(self):
        directory = self.parent_folder + self.target_folder
        for filename in os.listdir(directory):
            if filename.endswith(self.product+".hdf"):
                filename_raster = os.path.join(directory, filename)
                data_NASA = self.read_sub_datasets(filename_raster)
            else:
                continue
