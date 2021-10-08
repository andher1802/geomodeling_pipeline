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

from osgeo import ogr, osr, gdal
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
from rbf.interpolate import RBFInterpolant
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_points_griddata, rasterize_points_radial, rasterize_image
from pykrige.ok import OrdinaryKriging
from scipy import signal

from data_retrieval.knmi_data_source_files_retrieval import User_input_pipeline
from utils.set_up_database import set_up_database


class Knmi_Interpolator(User_input_pipeline):
    db_query_manager = None    
    raster_template = None
    boundaries = []
    width = 0
    height = 0
    interpolation_output_crs = None
    output_filename = None
    multiplier = 1

    def __init__(self, parent_folder='./data'):
        User_input_pipeline.__init__(self, parent_folder)
        super().__init__(parent_folder)
        self.db_query_manager = set_up_database()
        
    def interpolate_pipeline(self, columns):
        station_locations = self.__get_station_locations()
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        self.set_interpolation_boundaries()
        for current_day_to_download_relative in range(0, days_int):
            current_day_to_download_date = self.start_date + datetime.timedelta(current_day_to_download_relative)
            current_day_to_download_formatted = "'"+current_day_to_download_date.strftime('%Y%m%d')+"'"
            interpolation_params = {
                'column':'std,'+','.join(columns),
                'variable':'date', 
                'value':current_day_to_download_formatted
            }
            values_to_interpolate = self.__get_data_to_interpolate(interpolation_params)
            values_locations = pd.merge(station_locations, values_to_interpolate, on='STN')
            for column_in_df in columns:
                df_to_grid = values_locations[['LON', 'LAT', column_in_df.upper()]]
                df_to_grid_renamed = df_to_grid.rename(
                    columns = {'LON': 'x', 'LAT': 'y', column_in_df.upper():'value'}, inplace = False
                )
                self.set_filename_output(column_in_df.upper()+'_'+current_day_to_download_formatted[1:9]+'.tif')
                self.__interpolate(df_to_grid_renamed)
        
    def __interpolate(self, scattered_data):
        interpolation_method = self.algorithm['method']
        if (interpolation_method == 'invdist'):
            self.__interpolate_iwd(scattered_data)
        elif(interpolation_method == 'tps'):
            self.__interpolate_rbf(scattered_data)
        elif(interpolation_method == 'ordinary_kriging'):
            self.__interpolate_kriging(scattered_data)
        else:
            print('not interpolation method is recognized:', self.algorithm['method'])
        
    def __interpolate_iwd(self, scattered_data):
        interpolation_params_formatted = self.__get_interpolation_params()
        try:
            print('iwd_interpolation')    
            gridopt = gdal.GridOptions(
                format='GTiff',
                algorithm=interpolation_params_formatted,
                outputBounds = self.boundaries,
                outputSRS = self.interpolation_output_crs,
                width = self.width,
                height = self.height,
                outputType = gdal.GDT_Float32,
                z_multiply = self.multiplier
            )
            data_to_interpolate = scattered_data.dropna()
            data_to_interpolate.to_csv('points.csv', index=False, sep=';')
            gdal.Grid(self.output_filename, 'points.vrt', options=gridopt)
        except Exception as e: print(e)
        
    def __interpolate_rbf(self, scattered_data):
        interpolation_params_formatted = self.__get_interpolation_params()
        tps_params = interpolation_params_formatted.split(':')[1:]
        phi_param = tps_params[0].split('=')[1]
        order_param = float(tps_params[1].split('=')[1])
        sigma_param = float(tps_params[2].split('=')[1])
        try:
            print('rbf_interpolation')
            data_to_interpolate = scattered_data.dropna()
            data_to_interpolate['value'] = data_to_interpolate['value'] * self.multiplier
            x_obs = data_to_interpolate[['x', 'y']].to_numpy()
            u_obs = data_to_interpolate[['value']].to_numpy()
            u_obs = np.asarray([element[0] for element in u_obs])
            I = RBFInterpolant(x_obs, u_obs, sigma=sigma_param, phi=phi_param, order=order_param)
            x1, x2 = np.linspace(round(self.boundaries[0],3), round(self.boundaries[2],3), self.width),\
                        np.linspace(round(self.boundaries[3],3), round(self.boundaries[1],3), self.height)
            x_itp = np.reshape(np.meshgrid(x1, x2), (2, self.width*self.height)).T
            u_itp = I(x_itp)
            x_itp_df = pd.DataFrame(x_itp)
            x_itp_df['value'] = u_itp
            x_itp_df_rn = x_itp_df.rename(columns = {0: 'x', 1: 'y', 'value': 'value'}, inplace = False)
            x_itp_df_rn.to_csv(self.output_filename+'.csv')
            self.array2raster(x_itp_df_rn)
        except Exception as e: print(e)
    
    def __interpolate_kriging(self, scattered_data):
        print(scattered_data)
        interpolation_params_formatted = self.__get_interpolation_params()
        kriging_params = interpolation_params_formatted.split(':')[1:]
        sill_param = float(kriging_params[0].split('=')[1])
        range_param = float(kriging_params[1].split('=')[1])
        nugget_param = float(kriging_params[2].split('=')[1])
        bs_param = int(kriging_params[3].split('=')[1])
        try:
            print('kriging_interpolation')
            data_to_interpolate = scattered_data.dropna()
            data_to_interpolate['value'] = data_to_interpolate['value'] * self.multiplier
            OrdonaryKrigingInstance = OrdinaryKriging(
                data_to_interpolate[['x']],
                data_to_interpolate[['y']],
                data_to_interpolate[['value']],
                variogram_model="exponential",
                variogram_parameters = {'sill': sill_param, 'range':range_param, 'nugget':nugget_param},
                verbose=False,
                enable_plotting=False
            )
            x1, x2 = np.linspace(round(self.boundaries[0],3), round(self.boundaries[2],3), self.width),\
                    np.linspace(round(self.boundaries[1],3), round(self.boundaries[3],3), self.height)
            z, ss = OrdonaryKrigingInstance.execute("grid", x1, x2)
            z_filtered = self.__smmothing_array(z, bs_param)
            self.array2raster(np.array(z_filtered.T))
        except Exception as e: print(e)
            
    def __get_station_locations(self):
        self.db_query_manager.db_query_params['table'] = 'stn_metadata'
        station_locations = self.db_query_manager.get_data('*')
        df_station_locations = pd.DataFrame.from_records(station_locations, columns =['STN', 'ALT', 'LON', 'LAT', 'NAME'])
        df_station_locations['NAME'] = df_station_locations['NAME'].apply(lambda x: re.sub(r' +', '', x))
        return(df_station_locations)

    def __get_data_to_interpolate(self, condition):
        self.db_query_manager.db_query_params['table'] = 'test_phenology'
        values_to_interpolate = self.db_query_manager.get_data(condition['column'], condition)
        dataframe_titles = ['STN']
        dataframe_titles.extend(condition['column'].upper().split(',')[1:])
        df_to_interpolate = pd.DataFrame.from_records(values_to_interpolate, columns = dataframe_titles) 
        return(df_to_interpolate)
    
    def set_raster_template(self, raster_template):
        self.raster_template = raster_template
    
    def set_crs_output(self, interpolation_output_crs):
        self.interpolation_output_crs = interpolation_output_crs

    def set_interpolation_algorithm(self, interpolation_params):
        self.algorithm = interpolation_params

    def set_output_scale(self, multiplier):
        self.multiplier = multiplier    
        
    def set_filename_output(self, output_filename):
        self.output_filename = output_filename
    
    def set_interpolation_boundaries(self):
        raster_bestand = gdal.Open(self.raster_template)
        nr_rows = raster_bestand.RasterYSize
        nr_cols = raster_bestand.RasterXSize
        geotransform = raster_bestand.GetGeoTransform()
        x_origin = geotransform[0]
        y_origin = geotransform[3]
        cel_width = geotransform[1]
        cel_height = geotransform[5]
        raster_bestand = None
        raster_bestand = None
        ext_factor = 60
        width_rows_factor_added = ext_factor
        width_cols_factor_added = ext_factor
        x_ll = x_origin
        x_ll -=  width_cols_factor_added * cel_width
        x_ur = x_origin + nr_cols * cel_width
        x_ur +=  width_cols_factor_added * cel_width
        y_ur = y_origin
        y_ur -= width_rows_factor_added * cel_height
        y_ll = y_origin + nr_rows * cel_height
        y_ll += width_rows_factor_added * cel_height
        self.width = nr_cols + width_cols_factor_added * 2
        self.height = nr_rows + width_rows_factor_added * 2
        self.boundaries = [x_ll, y_ll, x_ur, y_ur]
        
    def array2raster(self, input_array):
        if isinstance(input_array, np.ndarray):
            gdf_pivot = input_array
        else:
            gdf_pivot = np.array(input_array.pivot(index='x', columns='y', values='value'))
        xmin,ymax,xmax,ymin = self.boundaries
        ncols = self.width
        nrows = self.height
        array_df = gdf_pivot
        xres = (xmax-xmin)/float(ncols)
        yres = (ymax-ymin)/float(nrows)
        geotransform = (xmin,xres,0,ymax,0, - yres)
        output_raster = gdal.GetDriverByName('GTiff').Create(str(self.output_filename), ncols, nrows, 1 ,gdal.GDT_Float32)
        output_raster.SetGeoTransform(geotransform)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(int(self.interpolation_output_crs.split(':')[1]))
        output_raster.SetProjection( srs.ExportToWkt() )
        output_raster.GetRasterBand(1).WriteArray(array_df.T)        
        output_raster.FlushCache()
    
    def __get_interpolation_params(self):
        list_params = []
        for key, value in self.algorithm.items():
            list_params.append(key+'='+value)
        interpolation_params = self.algorithm['method'] + ':' + ":".join(list_params[1:])
        return (interpolation_params)
    
    def __smmothing_array(self, input_array_2d, smoothing_block_size):
        filter_kernel = np.ones((smoothing_block_size, smoothing_block_size))/smoothing_block_size**2
        return signal.convolve2d(input_array_2d, filter_kernel, 'valid')