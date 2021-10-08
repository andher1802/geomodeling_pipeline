import os
import argparse
import datetime
import re
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
import rioxarray
import shapely
import pykrige.kriging_tools as kt

import dask.array as da

from osgeo import ogr, osr, gdal
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
from dateutil.relativedelta import *
from data_processing.knmi_interpolation import Knmi_Interpolator
   
class Knmi_compute(Knmi_Interpolator):    
    input_computation = []
    operation = None

    def __init__(self, parent_folder='./data'):
        Knmi_Interpolator.__init__(self, parent_folder)
        super().__init__(parent_folder)
   
    def set_inputs_computation(self, input_computation):
        self.inputs_computation = input_computation
    
    def set_operation(self, operation):
        self.operation = operation
        
    def raster_to_array(self, raster_name):
        try:
            raster_file = gdal.Open(raster_name)
            raster_values = raster_file.GetRasterBand(1).ReadAsArray()
            return raster_values
        except IOError:
            print("File not accessible")
        except Exception as e: print(e)
    
    def compute_sd(self, input_values):
        rh = input_values[0]
        t_sd = input_values[1]
        up = 0.0621 * t_sd
        p1 = (1 - np.divide(rh, 100))
        p2 = 4.9463 * np.exp(up)
        r = p1 * p2
        return np.transpose(np.round(r, decimals=2))
    
    def compute_vp(self, input_values):
        rh = input_values[0]
        t_vp = input_values[1]
        up = np.divide(17.27 * t_vp, 273.3 + t_vp)
        p1 = (1 - np.divide(rh, 100))
        p2 = 0.611 * np.exp(up)
        r = p1 * p2
        return np.transpose(np.round(r, decimals=2))
    
    def compute_result(self, input_values):
        if self.operation == 'compute_sd':
            result_raster = self.compute_sd(input_values)
        elif self.operation == 'compute_vp':
            result_raster = self.compute_vp(input_values)
        return result_raster
    
    def define_filename_output(self, current_day_to_download_formatted):
        if self.operation == 'compute_sd':
            self.set_target_folder('{0}/SD/'.format(self.parent_folder))
            return 'SD_{0}.tif'.format(current_day_to_download_formatted)
        elif self.operation == 'compute_vp':
            self.set_target_folder('{0}/VP/'.format(self.parent_folder))
            return 'VP_{0}.tif'.format(current_day_to_download_formatted)
        
    def compute_pipeline(self):
        self.set_interpolation_boundaries()
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        for current_day_to_download_relative in range(0, days_int):
            current_day_to_download_date = self.start_date +\
                                        datetime.timedelta(current_day_to_download_relative)
            current_day_to_download_formatted = current_day_to_download_date.strftime('%Y%m%d')
            rasters_to_compute = []
            for input_name in self.inputs_computation:
                filename_input = '{0}/{1}/{1}_{2}.tif'.format(\
                                                              self.parent_folder,\
                                                              input_name,\
                                                              current_day_to_download_formatted\
                                                             )
                rasters_to_compute.append(self.raster_to_array(filename_input))
            self.set_filename_output(self.define_filename_output(current_day_to_download_formatted))
            current_day_result = self.compute_result(rasters_to_compute)
            self.array2raster(current_day_result)
            print(self.output_filename)
            
class Knmi_create_features(Knmi_compute):
    var_name = None
    periods = []
    
    def __init__(self, parent_folder='./data'):
        Knmi_compute.__init__(self, parent_folder)
        super().__init__(parent_folder)
    
    def set_var_name(self, var_name):
        self.var_name = var_name
    
    def set_periods(self, periods):
        self.periods = list(periods)
    
    def compute_result(self, input_values):
        if self.operation == 'max':
            result_raster = np.max(raster_stack, axis=2)
        elif self.operation == 'min':
            result_raster = np.min(input_values, axis=2)
        elif self.operation == 'mean':
            result_raster = np.mean(input_values, axis=2)
        return np.transpose(result_raster) 
    
    def define_filename_output(self, current_day_to_download_formatted, period):
            self.set_target_folder('{0}/Features/{1}/'.format(self.parent_folder, self.var_name))
            return '{0}-{1}_{2}.tif'.format(self.var_name, str(period), current_day_to_download_formatted)

    def create_rasters(self, current_raster, current_days):
        for period in self.periods:
            limit = -1 * period
            stacked_array = np.dstack(current_raster[limit:])
            raster_avg = self.compute_result(stacked_array)
            self.set_filename_output(self.define_filename_output(current_days[-1], period))
            self.array2raster(raster_avg)
            print(self.output_filename)

    def compute_pipeline(self):
        self.set_interpolation_boundaries()
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        rasters_to_compute = []
        dates_to_compute = []
        last_ignored_day = None
        for current_day_to_download_relative in range(0, days_int + max(self.periods) + 1):
            current_day_analysis = self.start_date +\
                                        datetime.timedelta(current_day_to_download_relative)
            current_day_to_download_date = current_day_analysis - datetime.timedelta(max(self.periods))
            current_day_to_download_formatted = current_day_to_download_date.strftime('%Y%m%d')
            filename_input = '{0}/{1}/{1}_{2}.tif'.format(\
                                                          self.parent_folder,\
                                                          self.var_name,\
                                                          current_day_to_download_formatted\
                                                         )
            if not(os.path.isfile(filename_input)):
                last_ignored_day = current_day_analysis.strftime('%Y%m%d')
                continue
            else:
                rasters_to_compute.append(self.raster_to_array(filename_input))
                dates_to_compute.append(current_day_to_download_formatted)
                if len(rasters_to_compute) >= max(self.periods):
                    self.create_rasters(rasters_to_compute, dates_to_compute)

class MODIS_Compute(Knmi_create_features):
    product = None
    vars_names = []  
    
    def __init__(self, parent_folder='./data'):
        Knmi_create_features.__init__(self, parent_folder)
        super().__init__(parent_folder)

    def set_product(self, product):
        self.product = product

    def set_vars_names(self, vars_names):
        self.vars_names = vars_names
   
    def array_to_tif(self, data_to_save):
        band_array = data_to_save['band_array']
        print(band_array)
        band_ds = data_to_save['template']
        band_path = self.output_filename
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

    def NDWI_pipeline(self, dt):
        rasters = ['{0}_{1}_{2}.tiff'.format(dt, self.product, element) for element in self.vars_names]
        raster_nir_filename = rasters[0]
        raster_sw_filename = rasters[1]
        raster_template = gdal.Open(raster_nir_filename)
        raster_nir = self.raster_to_array(raster_nir_filename)
        raster_sw = self.raster_to_array(raster_sw_filename)
        array_ndwi = (raster_nir.astype(float) - raster_sw.astype(float))/(raster_nir.astype(float) + raster_sw.astype(float))
        self.set_filename_output('{0}_{1}_{2}.tiff'.format(dt, self.product, 'ndwi'))
        self.array_to_tif({'band_array':array_ndwi, 'template':raster_template})
        print(self.output_filename)

    def compute_NDWI(self):
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        rasters_to_compute = []
        dates_to_compute = []
        last_ignored_day = None
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        start_date_filter = self.start_date.strftime('%Y/%m/%d')
        end_date_filter = self.end_date.strftime('%Y/%m/%d')
        range_dates = pd.date_range(start_date_filter, end_date_filter, freq="MS")
        diff_months = len(range_dates)
        for i in range(1, diff_months + 1):
            today = self.start_date
            j = i - 1
            d = today.replace(day=1) + relativedelta(months=j)
            dt = d.strftime('%Y.%m.%d')
            self.NDWI_pipeline(dt)

    def compute_aggregated_stats(self):
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        start_date_filter = self.start_date.strftime('%Y')
        end_date_filter = self.end_date.strftime('%Y')
        for year in range(int(start_date_filter), int(end_date_filter)+1):
            os_generator = os.walk(".")
            da_list = []
            for root, dirs, file_list in os_generator:
                for file_name in file_list:
                    if file_name.find(str(year)) != -1:
                        da_raster_content = self.raster_to_array(file_name)
                        da_list.append(da_raster_content)
            da_ensembled_raster = np.stack(da_list, axis=-1)
            raster_result_max = np.apply_along_axis(np.max, 2, da_ensembled_raster)
            raster_result_min = np.apply_along_axis(np.min, 2, da_ensembled_raster)
            raster_result_range = raster_result_max - raster_result_min
            raster_template = gdal.Open(file_name)
            self.set_filename_output('{0}_{1}_{2}.tiff'.format(year, self.target_folder, 'min'))
            self.array_to_tif({'band_array':raster_result_min, 'template':raster_template})
            self.set_filename_output('{0}_{1}_{2}.tiff'.format(year, self.target_folder, 'max'))
            self.array_to_tif({'band_array':raster_result_max, 'template':raster_template})
            self.set_filename_output('{0}_{1}_{2}.tiff'.format(year, self.target_folder, 'range'))
            self.array_to_tif({'band_array':raster_result_range, 'template':raster_template})
            print('rasters created successfully')

class LGN_Compute(Knmi_create_features):

    def __init__(self, parent_folder):
        Knmi_create_features.__init__(self, parent_folder)
        super().__init__(parent_folder)

    def resample(self):
        return 0

    def reclassify(self):
        return 0
