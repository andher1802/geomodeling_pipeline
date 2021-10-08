from data_processing import knmi_interpolation, knmi_computation, feature_extraction
from data_retrieval import knmi_data_source_files_retrieval, modis_retrieval
from utils import api_keys, data_base_management

import set_arguments_pipeline
import re


def main():

    input_arguments = set_arguments_pipeline.set_arguments_pipeline()
    
    DB_params = {
        'PGHOST': api_keys.PGHOST,
        'PGDATABASE': api_keys.PGDATABASE,
        'PGUSER': api_keys.PGUSER,
        'PGPASSWORD': api_keys.PGPASSWORD,
        'TIMEOUT': api_keys.TIMEOUT,
        'PORT': api_keys.PORT
    }

    # Download KNMI Source Files
    data_retrieval_handler = knmi_data_source_files_retrieval.Knmi_data_source_retrieval('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/')
    data_retrieval_handler.set_end_date(input_arguments['end_date'])
    data_retrieval_handler.set_start_date(input_arguments['start_date'])
    data_retrieval_handler.set_target_folder(input_arguments['folder'])
    data_retrieval_handler.download_knmi_source_files(['TN','TX','RH','EV24','UG','TG'])

    # Download KNMI Interpolated rasters
    # names 'Tn1_', ...
#     raster_dataset = {
#         'name':'EV24',
#         'version':'2'
#     }
        
#     data_retrieval_handler_rasters = knmi_data_source_files_retrieval.Knmi_interpolated_raster_retrieval('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/')
#     data_retrieval_handler_rasters.set_dataset_raster(raster_dataset)
#     data_retrieval_handler_rasters.set_end_date(input_arguments['end_date'])
#     data_retrieval_handler_rasters.set_start_date(input_arguments['start_date'])
#     data_retrieval_handler_rasters.set_target_folder(input_arguments['folder'])
#     data_retrieval_handler_rasters.set_api_key(api_keys.api_keys)
#     data_retrieval_handler_rasters.download_knmi_interpolated_rasters()

#     ### Create table in db for knmi raw data

#     db_setting_manager = data_base_management.DB_manager(DB_params)

#     table_name = 'test_phenology'
#     table_params = [
#         {
#             'name_field':'STD' , 
#             'type_field':'INT NOT NULL'
#         },
#         {
#             'name_field':'DATE' , 
#             'type_field':'CHAR(8) NOT NULL'
#         },
#         {
#             'name_field':'TN' , 
#             'type_field':'INT'
#         },
#         {
#             'name_field':'TX' , 
#             'type_field':'INT'
#         },
#         {
#             'name_field':'RH' , 
#             'type_field':'INT'
#         },
#         {
#             'name_field':'EV24' , 
#             'type_field':'INT'
#         },
#                 {
#             'name_field':'UG' , 
#             'type_field':'INT'
#         },
#         {
#             'name_field':'TG' , 
#             'type_field':'INT'
#         }
#     ]

#     db_setting_manager.create_data_base_table(table_name, table_params)
#     query_params = {
#         'columns': '*',
#         'schema': 'andres',
#         'table': 'stn_metadata'
#     }

#     ### Populate db table with the knmi text files

#     db_query_manager = data_base_management.DB_query(db_setting_manager.get_conn(), query_params)
#     data = db_query_manager.get_data('*')
#     with open('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Stations_Metadata/station_coordinates.csv') as stn_metadata:
#         lines_info = stn_metadata.readlines()
#         insert_data = []
#         for line in lines_info[1:]:
#             line = line.strip().split(",")
#             line[0] = re.sub(r'"', '', line[0])
#             line[2] = re.sub(r'"', "'", line[2])
#             line[2] = re.sub(r'-', " ", line[2])
#             print(line)
#             insert_data.append(line)
        
#     insert_data = [['1'],['2']]
#     db_query_manager.insert_data(insert_data)

#     ### Create interpolated rasters

#     Interpolator = knmi_interpolation.Knmi_Interpolator('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/')
    
#     Interpolator.set_start_date(input_arguments['start_date'])
#     Interpolator.set_end_date(input_arguments['end_date'])
#     Interpolator.set_target_folder(input_arguments['folder'])
    
#     Interpolator.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#     Interpolator.set_crs_output('EPSG:28992')

#     Interpolator.set_output_scale(0.1)
    
#     interpolation_params = {'method':'invdist', 'power':'2.5', 'smoothing':'10000'}
#     Interpolator.set_interpolation_algorithm(interpolation_params)
#     columns = ['tg','tn','tx']
#     Interpolator.interpolate_pipeline(columns)
   
#     smoothing_tps = 43496 * 2
#     interpolation_params = {'method':'tps', 'phi':'phs2', 'order':'1', 'sigma':str(smoothing_tps)}
#     Interpolator.set_interpolation_algorithm(interpolation_params)
#     columns = ['ev24',]
#     Interpolator.interpolate_pipeline(columns)
        
#     range_krig = 121505 * 43496
#     interpolation_params = {'method':'ordinary_kriging', 'sill':'0.04', 'range':str(range_krig), 
#                             'nugget':'0', 'smoothing':str(20)}
#     Interpolator.set_interpolation_algorithm(interpolation_params)
#     Interpolator.set_output_scale(1)
#     columns = ['ug']
#     Interpolator.interpolate_pipeline(columns)

#     Interpolator.set_output_scale(1)
#     columns = ['rh']
#     Interpolator.interpolate_pipeline(columns)

#     ### Compute SD and VP

#     Computing_raster = knmi_computation.Knmi_compute(\
#                         '/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Self_Interpolated_Data')
#     Computing_raster.set_start_date(input_arguments['start_date'])
#     Computing_raster.set_end_date(input_arguments['end_date'])
#     Computing_raster.set_target_folder(input_arguments['folder'])
#     Computing_raster.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#     Computing_raster.set_crs_output('EPSG:28992')
    
#     Computing_raster.set_inputs_computation(['UG', 'TG'])
#     Computing_raster.set_operation('compute_sd')
#     Computing_raster.compute_pipeline()
    
#     Computing_raster.set_operation('compute_vp')
#     Computing_raster.compute_pipeline()

#     ### FEATURE CREATION (Averaging rasters by period)

#     Features = knmi_computation.Knmi_create_features(\
#                         '/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Self_Interpolated_Data')
#     Features.set_start_date(input_arguments['start_date'])
#     Features.set_end_date(input_arguments['end_date'])
#     Features.set_target_folder(input_arguments['folder'])
#     Features.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#     Features.set_crs_output('EPSG:28992')
#     Features.set_operation('mean')
    
#     Features.set_var_name('EV24')
#     Features.set_periods([1, 2, 3, 5, 6, 7, 14, 365])    
#     Features.compute_pipeline()
    
#     Features.set_var_name('UG')
#     Features.set_periods([1, 2, 3, 4, 7, 14, 30, 90, 365])    
#     Features.compute_pipeline()

#     Features.set_var_name('SD')
#     Features.set_periods([2, 3, 4, 5, 6, 7, 14, 90, 365])    
#     Features.compute_pipeline()
    
#     Features.set_var_name('RH')
#     Features.set_periods([14, 90, 365])    
#     Features.compute_pipeline()
    
#     Features.set_var_name('TN')
#     Features.set_periods([90, 365])
#     Features.compute_pipeline()

#     Features.set_var_name('TX')
#     Features.set_periods([90, 365])    
#     Features.compute_pipeline()

#     ### Download MODIS data using GEE

#     modisInstance = modis_retrieval.ModisRetrieval('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/')
#     modisInstance.set_end_date(input_arguments['end_date'])
#     modisInstance.set_start_date(input_arguments['start_date'])
#     modisInstance.set_target_folder(input_arguments['folder'])
#     modisInstance.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#     modisInstance.set_crs_output('EPSG:28992')
#     modisInstance.set_vars_names(['NDVI', 'EVI'])

     ### Download MODIS data using NASA
#     NasaInstance = modis_retrieval.NASARetrieval('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/NASA/HDF/')
#     NasaInstance.set_end_date(input_arguments['end_date'])
#     NasaInstance.set_start_date(input_arguments['start_date'])
#     NasaInstance.set_target_folder(input_arguments['folder'])
   ### SET PRODUCT
#     NasaInstance.set_product('MOD13A3.006')
#     NasaInstance.set_type_data('MOLT')
#     NasaInstance.set_tiles('h18v03')
#     NasaInstance.set_urs('urs.earthdata.nasa.gov')
#     NasaInstance.set_up_credentials()
#     NasaInstance.derive_file_list()
#     NasaInstance.download_files()
   ## NASA HDF to TIFF
#     NasaInstance.set_vars_names(['1 km monthly NDVI', '1 km monthly EVI'])
#     NasaInstance.hdf_to_tiff()

#    NasaInstance.set_product('MCD43A1.006')
#    NasaInstance.set_tiles('h18v03')
#    NasaInstance.set_type_data('MOTA')
    # SET CREDENTIALS FIRST
#    NasaInstance.set_urs('urs.earthdata.nasa.gov')
#    NasaInstance.set_up_credentials()
#    NasaInstance.derive_file_list()
#    NasaInstance.download_files()

    ## NASA HDF to TIFF
#    NasaInstance.set_vars_names([
#        'BRDF_Albedo_Band_Mandatory_Quality_Band1',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band2',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band3',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band4',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band5',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band6',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band7',
#        'BRDF_Albedo_Band_Mandatory_Quality_nir', 
#        'BRDF_Albedo_Band_Mandatory_Quality_shortwave'
#        ])
#    NasaInstance.hdf_to_tiff()

    # Compute NDWI from NASA MCD43A1

#    Features_NDWI = knmi_computation.MODIS_Compute('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/NASA/TIFF')
#    Features_NDWI.set_start_date(input_arguments['start_date'])
#    Features_NDWI.set_end_date(input_arguments['end_date'])
#    Features_NDWI.set_target_folder(input_arguments['folder'])
#    Features_NDWI.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#    Features_NDWI.set_crs_output('EPSG:28992')
#    Features_NDWI.set_product('MCD43A1.006')
#    Features_NDWI.set_vars_names([
#        'BRDF_Albedo_Band_Mandatory_Quality_Band2',
#        'BRDF_Albedo_Band_Mandatory_Quality_Band6'
#        'BRDF_Albedo_Band_Mandatory_Quality_nir', 
#        'BRDF_Albedo_Band_Mandatory_Quality_shortwave'
#        ])
#    Features_NDWI.compute_NDWI()

    # Create Features for NASA
#    Features_NDWI = knmi_computation.MODIS_Compute('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Self_Interpolated_Data')
#    Features_NDWI.set_start_date(input_arguments['start_date'])
#    Features_NDWI.set_end_date(input_arguments['end_date'])
#    Features_NDWI.set_target_folder(input_arguments['folder'])
#    Features_NDWI.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#    Features_NDWI.set_crs_output('EPSG:28992')
#    Features_NDWI.compute_aggregated_stats()
    
    # Get landuse map

    # Extract values
#    Extract_values = feature_extraction.Feature_collector('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Self_Interpolated_Data/Features')
#    Extract_values.set_start_date(input_arguments['start_date'])
#    Extract_values.set_end_date(input_arguments['end_date'])
#    Extract_values.set_target_folder(input_arguments['folder'])
#    Extract_values.set_raster_template('/home/jupyter-andres/Andres_Folder/Phenology_data/Validation_Files/nederland_1000.tif')
#    Extract_values.set_crs_output('EPSG:28992')
#    coordinate_list = [
#                 (6.34, 52.93),#'Appelscha' 
#                 (5.23, 52.15),#'Bilthoven' 
#                 (5.69, 52.53),#'Dronten' 
#                 (5.69, 52.03),#'Ede' 
#                 (6.76, 53.02),#'Gieten' 
#                 (5.87, 52.11),#'HoogBaarlo' 
#                 (3.98, 51.84),#'KwadeHoek' 
#                 (6.22, 51.89),#'Montferland' 
#                 (6.42, 52.34),#'Nijverdal' 
#                 (6.16, 53.49),#'Schiermonnikoog' 
#                 (4.89, 52.45),#'Twiske' 
#                 (5.96, 50.79),#'Vaals' 
#                 (5.33, 51.42),#'Veldhoven' 
#                 (4.36, 52.16),#'Wassenaar' 
#            ]
#    Extract_values.activate_transform()
#    Extract_values.set_extraction_coordinates(coordinate_list)
#    Extract_values.extract_features_from_folder()

    ## assemble features into one file
#    Merge_Features = feature_extraction.Feature_merge('/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/Self_Interpolated_Data/Features')
#    Merge_Features.feature_merge()

if __name__ == '__main__':
    main()
