from data_processing import knmi_interpolation, knmi_computation, feature_extraction
from data_retrieval import knmi_data_source_files_retrieval, modis_retrieval
from utils import api_keys, data_base_management

import set_arguments_pipeline
import re


def main():
    input_arguments = set_arguments_pipeline.set_arguments_pipeline()
   
    # Download KNMI Source Files
    parent_folder = '/home/jupyter-andres/Andres_Folder/Phenology_data/Datasets/'
    knmi_stations_data_collector = knmi_data_source_files_retrieval.knmi_collector(parent_folder)
    knmi_stations_data_collector.set_end_date(input_arguments['end_date'])
    knmi_stations_data_collector.set_start_date(input_arguments['start_date'])
    knmi_stations_data_collector.set_target_folder(input_arguments['folder'])
    knmi_stations_data_collector.read_knmi_all_stations_file().compute()
    knmi_stations_data_collector.remove_temp_files()
    knmi_stations_data_collector.add_head_files()
    knmi_stations_data_collector.remove_temp_files('csv')
    
if __name__ == '__main__':
    main()

