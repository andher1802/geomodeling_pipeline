import datetime
import requests
import os
import argparse
import re

import numpy as np
import dask
import dask.array as da
import dask.bag as db
import gdal, ogr, gdalnumeric, gdalconst

from pathlib import Path
from utils.set_up_database import set_up_database


class User_input_pipeline(object):
    start_date = None
    end_date = None
    parent_folder = ''
    target_folder = ''

    def __init__(self, parent_folder='./data'):
        self.parent_folder = parent_folder
        self._check_create_folder(self.parent_folder)
    
    def set_start_date(self, start_date):
        self.start_date = start_date 
        
    def set_end_date(self, end_date):
        self.end_date = end_date
        
    def set_target_folder(self, target_folder):
        self.target_folder = target_folder
        self._check_create_folder(self.target_folder)
        
    def _check_create_folder(self, folder_name):
        if not os.path.isdir(folder_name):
            os.mkdir(folder_name)
        os.chdir(folder_name)        
        

class Knmi_data_source_retrieval(User_input_pipeline):
    def __init__(self, parent_folder='./data'):
        User_input_pipeline.__init__(self, parent_folder)
        super().__init__(parent_folder)
                
    def download_knmi_source_files(self, variables):
        base_url = 'http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi'
#         base_url = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/jaar.zip'
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        variables_download = ':'.join(variables)
        current_day_to_download_end = self.start_date + datetime.timedelta(days_int)
        current_day_to_download_formatted = self.start_date.strftime('%Y%m%d')
        current_day_to_download_end_formatted = current_day_to_download_end.strftime('%Y%m%d')
        http_request_params = {
        'stns': 'ALL',
        'start': current_day_to_download_formatted,
        'end': current_day_to_download_end_formatted,
        'vars': variables_download
        }
        http_knmi_source_request_handler = Knmi_http_request_handler(base_url)
        http_knmi_source_request_handler.set_http_request_params(http_request_params)
        try:
            response = http_knmi_source_request_handler.handle_http_request()
            result = response.text
            self.__text_to_database(result)
            fn = 'knmi_source_' + current_day_to_download_formatted + '_to_' + current_day_to_download_end_formatted + '.txt'
            outFile=open(fn, "w")
            outFile.write(result)
            outFile.close()
            print ('file has been created : ' + fn)
        except Exception as e: print(e)
    
    def __text_to_database(self, input_raw_data):
        raw_data_split = input_raw_data.split('\n')
        db_query_manager = set_up_database()
        insert_dataset = []
        for line in raw_data_split:
            line = re.sub(r" +", "", line)
            if len(line) > 1 and line[0] != '#':
                line_format = line.rstrip().split(',')
                line_format_with_null = [x if x != '' else 'NULL' for x in line_format]
                insert_dataset.append(line_format_with_null)
                print(line)
        db_query_manager.insert_data(insert_dataset)

        
class Knmi_interpolated_raster_retrieval(User_input_pipeline):    
    api_key = ''
    dataset_raster = {}

    def __init__(self, parent_folder='./data'):
        User_input_pipeline.__init__(self, parent_folder)
        super().__init__(parent_folder)
    
    def set_dataset_raster(self, dataset_raster):
        self.dataset_raster = dataset_raster

    def set_api_key(self, api_key):
        self.api_key = api_key[self.dataset_raster['name']]
    
    def __get_api_list_files(self):
        api_url = 'https://api.dataplatform.knmi.nl/open-data'
        api_version = 'v1'
        dataset_name = self.dataset_raster['name']
        dataset_version = self.dataset_raster['version']
        ensembled_url = f'{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files'
        timestamp_pattern = self.start_date.strftime('%Y%m%d')
        dataset_name_upper = dataset_name.upper()
        start_after_filename_prefix = f'INTER_OPER_R___{dataset_name_upper}____L3__{timestamp_pattern}'
        ensembled_url = f'{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files'
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days        
        if days_int < 1:
            raise Exception('Start date should be set before end date')
        http_request_params = {
            'maxKeys': str(days_int + 1),
            'Access-Control-Allow-Origin': '*',        
            'startAfterFilename': start_after_filename_prefix
        }
        http_request_headers = {'Authorization': self.api_key}
        http_raster_list_request_handler = Knmi_http_request_handler(ensembled_url)
        http_raster_list_request_handler.set_http_request_params(http_request_params)
        http_raster_list_request_handler.set_http_request_headers(http_request_headers)      
        list_files_response = http_raster_list_request_handler.handle_http_request()
        list_files = list_files_response.json()
        dataset_files = list_files.get('files')
        return dataset_files
    
    def __get_temporary_download_url(self, file):
        filename = file.get('filename')
        api_url = 'https://api.dataplatform.knmi.nl/open-data'
        api_version = 'v1'
        dataset_name = self.dataset_raster['name']
        dataset_version = self.dataset_raster['version']
        endpoint = f'{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url'
        http_request_headers = {'Authorization': self.api_key}
        http_raster_url_request_handler = Knmi_http_request_handler(endpoint)
        http_raster_url_request_handler.set_http_request_headers(http_request_headers)      
        url_response = http_raster_url_request_handler.handle_http_request()
        download_url = url_response.json().get('temporaryDownloadUrl')
        return download_url
    
    def __download_knmi_raster(self, file, knmi_url_download):
        filename = file.get('filename')
        http_raster_request = Knmi_http_request_handler(knmi_url_download)
        url_response = http_raster_request.handle_http_request()
        p = Path(filename)
        p.write_bytes(url_response.content)
        print(f'{filename} created successfully')
                
    def download_knmi_interpolated_rasters(self):
        list_rasters_download = self.__get_api_list_files()
        for file in list_rasters_download:
            download_url = self.__get_temporary_download_url(file)
            self.__download_knmi_raster(file, download_url)

class Knmi_http_request_handler(object):
    base_url_request = ''
    params_url_request = {}
    headers_url_request = {}
    
    def __init__(self, base_url):
        self.base_url_request = base_url
    
    def set_http_request_params(self, params_url_request):
        self.params_url_request = params_url_request

    def set_http_request_headers(self, headers_url_request):
        self.headers_url_request = headers_url_request
        
    def handle_http_request(self):
        try:
            r = requests.get(
                self.base_url_request, 
                headers = self.headers_url_request, 
                params = self.params_url_request
            )
            r.raise_for_status()
            return r.content
        except requests.exceptions.HTTPError as e:
            print (e.response.text)  

class knmi_collector(User_input_pipeline):
    collect_climate_vars_names = ['# STN', 'YYYYMMDD', 'TN', 'TX', 'RH', 'EV24', 'UG', 'TG']
    def __init__(self, parent_folder):
        User_input_pipeline.__init__(self, parent_folder)
        super().__init__(parent_folder)
    
    def read_knmi_all_stations_file(self):
        base_url = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/jaar.zip'
        temp_filename = 'temp_data.zip'
        http_request_params = {}
        http_knmi_source_request_handler = Knmi_http_request_handler(base_url)
        http_knmi_source_request_handler.set_http_request_params(http_request_params)
        try:
            response = http_knmi_source_request_handler.handle_http_request()
            result = response
            with open(temp_filename, 'wb') as compressed_knmi_data:
                compressed_knmi_data.write(result)
            raw_knmi_station_data = db.read_text(temp_filename, compression='zip')
            cleaned_knmi_station_data = self.clean_knmi_raw_data(raw_knmi_station_data)
            filtered_dataframe = self.get_knmi_columns(cleaned_knmi_station_data)
            for str_date in self.get_current_last_dates():
                filtered_dates = self.filter_dates_knmi(filtered_dataframe, str_date)
                filename_current = '{0}.csv'.format(str_date)
                filtered_dates.to_csv(filename_current, index=False, single_file = True)
            return filtered_dates
        except Exception as e: print(e)
            
        
    def clean_knmi_raw_data(self, raw_data):
        stripped_rows = db.map(lambda x:x.lstrip(), raw_data)
        stripped_no_blanks = stripped_rows.filter(lambda x:x!='')
        filtered_knmi_data = self.get_knmi_current_stations_data(stripped_no_blanks)
        return(filtered_knmi_data)
    
    def get_knmi_current_stations_data(self, knmi_data_bag):
        filtered_csv_no_csv = knmi_data_bag.filter(lambda x:re.match('[^0-9]', x[0]))
        filtered_csv_only_titles = knmi_data_bag.filter(lambda x:x[0]=='#')
        filtered_csv_only_values = knmi_data_bag.filter(lambda x:re.match('[0-9]', x[0]))
        titles_row = filtered_csv_only_titles.map(lambda x: x.strip().split(',')).compute()[0]
        titles_row_strip = list(map(lambda x:x.lstrip(), titles_row))
        tiles_dict = dict(zip(range(len(titles_row_strip)), titles_row_strip)) 
        knmi_stations_dataframe_allyear = filtered_csv_only_values.map(lambda x: x.strip().split(',')).to_dataframe()
        knmi_stations_dataframe_allyear_renamed = knmi_stations_dataframe_allyear.rename(columns=tiles_dict)
        return knmi_stations_dataframe_allyear_renamed
    
    def get_knmi_columns(self, knmi_all_columns):
        knmi_filtered_columns = knmi_all_columns[self.collect_climate_vars_names]
        return knmi_filtered_columns

    def filter_dates_knmi(self, knmi_filtered_columns, str_date):
        filetered_dates = knmi_filtered_columns[knmi_filtered_columns.YYYYMMDD == str_date]
        return filetered_dates

    def get_current_last_dates(self):
        todays_date = datetime.date.today()
        dates_list = []
        days_timedelta = self.end_date - self.start_date
        days_int = days_timedelta.days
        if days_int < 0:
            raise Exception('Start date should be set before end date')
        for days_past in range(1, days_int):
            current_date = self.end_date - datetime.timedelta(days_past)
            dates_list.append(datetime.datetime.strftime(current_date, '%Y%m%d'))
        return dates_list
    
    def remove_temp_files(self, wild_card = 'zip'):
        test = os.listdir('.')
        for item in test:
            if item.endswith("{0}".format(wild_card)):
                os.remove(item)
    
    def add_head_files(self):
        absolute_path = os.path.join(self.parent_folder, self.target_folder)
        filenames = [file for file in os.listdir('.') if file.split('.')[1] == 'csv']
        for file in filenames:
            with open(file, 'r') as input_file:
                with open('coordinate_info.txt', 'r') as coordinate_file:
                    with open('{0}.txt'.format(file.split('.')[0]),'w') as newf:
                        newf.write(coordinate_file.read())
                        newf.write(input_file.read())
            