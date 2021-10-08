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
import struct

import dask.array as da

import  multiprocessing

from osgeo import ogr, osr, gdal
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
from dateutil.relativedelta import * 
from data_processing.knmi_interpolation import Knmi_Interpolator
from osgeo.osr import SpatialReference, CoordinateTransformation
 
class Coordinate_reference():
    epsg_from = SpatialReference()
    epsg_to = SpatialReference()
    to2from = None
    from2to = None

    def __init__(self, from_cs, to_cs):
        self.epsg_from.ImportFromEPSG(from_cs)
        self.epsg_to.ImportFromEPSG(to_cs)
    
    def set_wgs84_from_epsg28992(self):
        self.epsg_from.SetTOWGS84(565.237,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812)
    
    def define_translator(self):
        self.from2to = CoordinateTransformation(self.epsg_from, self.epsg_to)
        self.to2from = CoordinateTransformation(self.epsg_to, self.epsg_from)
    
    def transformPoints_to_target(self, coordinates):
        coordinates_in_origin_cs = self.from2to(coordinates[0], coordinates[1])
        return coordinates_in_origin_cs 

    def transformPoints_to_origin(self, coordinates):
        latitude = coordinates[1]
        longitude = coordinates[0]
        northing, easting, z = self.to2from.TransformPoint(longitude, latitude)
        return (easting, northing)
        

class Feature_collector(Knmi_Interpolator):    
    product = None
    transform = False
    coordinates =[]
    vars_names = []  
    aggregated_features = ['EVI', 'NDVI', 'NDWI']

    def __init__(self, parent_folder='./data'):
        Knmi_Interpolator.__init__(self, parent_folder)
        super().__init__(parent_folder)
    
    def activate_transform(self):
        self.transform = True

    def set_extraction_coordinates(self, coordinates):
#         print('Coordinates transformation')
#         print(coordinates)
        if self.transform:
            translate_coordinates = Coordinate_reference(28992, 4326)
            translate_coordinates.set_wgs84_from_epsg28992()
            translate_coordinates.define_translator() 
            self.coordinates = [translate_coordinates.transformPoints_to_origin(individual_coordinate) for individual_coordinate in coordinates]
        else:
            self.coordinates = coordinates
#         print(self.coordinates)
#         print('------------------------')

    def extract_features_from_folder(self):
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        os_generator = os.walk(".")
        feature_list = []
        for root, dirs, file_list in os_generator:
            for file_name in file_list[:]:
                if self.target_folder in self.aggregated_features:
                    extracted_values = self.extract_features_from_raster(file_name)
                    start_date_filter = self.start_date.strftime('%Y')
                    end_date_filter = self.end_date.strftime('%Y')
                    filename_info = file_name[:-5].split('_')
                    feature_extracted = filename_info[1]+'-'+filename_info[2] 
                    date_extracted = filename_info[0]
                    feature_list.extend([[feature_extracted, date_extracted, value[0], value[1], value[2]] for value in extracted_values])
                else:
                    extracted_values = self.extract_features_from_raster(file_name)
                    start_date_filter = self.start_date.strftime('%Y%m%d')
                    end_date_filter = self.end_date.strftime('%Y%m%d')
                    filename_info = file_name[:-4].split('_')
                    feature_extracted = filename_info[0]
                    date_extracted = filename_info[1]
                    feature_list.extend([[feature_extracted, date_extracted, value[0], value[1], value[2]] for value in extracted_values])
        filename_out = '../{0}_feature.csv'.format(self.target_folder) 
        with open(filename_out, 'w') as output_file:
            for line in feature_list:
                print(';'.join([x if isinstance(x, str) else '{:.3f}'.format(x) for x in line]), file=output_file)

    def extract_features_from_raster(self, file_name):
        raster_load = gdal.Open(file_name)
        gt = raster_load.GetGeoTransform()
        img_width, img_height = raster_load.RasterXSize, raster_load.RasterYSize
        raster_values = raster_load.GetRasterBand(1)
        x_origin = gt[0]
        y_origin = gt[3] 
        pixelWidth = gt[1]
        pixelHeight = gt[5]
        extracted_values = []
        for northing, easting in self.coordinates:
            px2 = int((easting - x_origin) / pixelWidth)
#             py2 = raster_load.RasterYSize - int((northing - y_origin) / pixelHeight)
            py2 = int((northing - y_origin) / pixelHeight)
            struct_values = raster_values.ReadRaster(px2, py2, 1, 1,  buf_type=gdal.GDT_Float32)
            intval = struct.unpack('f' , struct_values)
            extracted_values.append([easting, northing, intval[0]])
        return extracted_values

class Feature_merge(Knmi_Interpolator):
    def __init__(self, parent_folder='./data'):
        Knmi_Interpolator.__init__(self, parent_folder)
        super().__init__(parent_folder)

    def feature_merge(self):
        pass

