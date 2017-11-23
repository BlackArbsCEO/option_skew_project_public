import os
import pandas as pd
import numpy as np

import pyarrow as pa
import pyarrow.parquet as pq

class file_handler:
    """
    class for handling directory/folder creation + data saving

    # Attributes
        project_dir : str(), main project directory

    # Methods
        save_data : actual public save function
            |__> _create_dir : internal fn to create dir if it does not exist
                |_> __check_exists_or_create : private fn to check if file exists
            |__> __create_date_str : private fn to create date str
            |__> __create_timestamp_str : private fn to create timestamp str

    """
    def __init__(self, project_dir):
        self.project_dir = project_dir

    def __check_exists_or_create(self, _dir):
        """fn: to check if file/path exists"""
        if not os.path.exists(_dir):
            try:
                os.mkdir(_dir)
            except Exception as e:
                print(e)
        return

    def _create_dir(self):
        """fn: create daily directory if not already created"""
        _dir = self.project_dir+'/Yahoo_Options_Data/'+str(pd.to_datetime('now').date())+'/'

        self.__check_exists_or_create(_dir)
        return _dir

    def __create_timestamp_str(self):
        """fn: to create time stamp str"""
        return str(pd.to_datetime('now').tz_localize('utc').tz_convert('US/Eastern')).replace(' ', '_').replace(':','.')

    def __create_date_str(self):
        """fn: to create date str"""
        return str(pd.to_datetime('now').date())

    def save_data(self, data, format='parquet', resolution='time', errors=False):
        """fn: to save data to directory

        # Args
            data : pd.DataFrame
            format : str, ('parquet', 'h5', 'csv', 'feather')
            resolution : str, date or time
                if date uses default str format,
                if time will use YYYY-MM-DD_HH.MM.SS
            errors : bool,
                if True change filepath name
                if False use options data filepath name
        """
        _dir = self._create_dir()

        if resolution=='time':
            _timestamp = self.__create_timestamp_str()
        elif resolution=='date':
            _timestamp = self.__create_date_str()

        if errors:
            _fp = _dir + f'yahoo_options_scraper_errors_{_timestamp}.{format}'
        else:
            _fp = _dir + f'yahoo_options_data_{_timestamp}.{format}'

        if format=='parquet':
            _table = pa.Table.from_pandas(data)
            pq.write_table(_table, _fp)

        elif format == 'h5': data.to_hdf(_fp, key='data')
        elif format == 'csv': data.to_csv(_fp, index=False)
        elif format == 'feather': data.to_feather(_fp)
        return
