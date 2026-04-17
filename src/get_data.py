# %%
import pandas as pd
import os
import os.path as osp
import numpy as np
import json, requests, argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import config_logger

# %%
try:
    PROJECT_ROOT = osp.abspath(osp.join(osp.dirname(__file__),'..'))
except:
    PROJECT_ROOT = osp.abspath(osp.join(os.getcwd(),'..'))

logger = config_logger.get_logger(__name__)
logger.info(PROJECT_ROOT)

# %%
_description = f""" 
Télécharge les données météo Australien depuis le site bom.gov.au

"""
parser = argparse.ArgumentParser(description='\n'.join([_description]))
parser.add_argument('--city', type=str, required=True, help="City name to get weather report")
parser.add_argument('--maxlag', type=str, required=False, default=3, help="Maximum lag computed in features model")

# %%
def get_stations_infos():
    stations_file = osp.join(PROJECT_ROOT,'src','stations_infos.json')
    if not osp.exists(stations_file):
        logger.error(f'{stations_file} not found !')
        raise FileNotFoundError
    
    logger.info(f'Load {osp.basename(stations_file)}')
    with open(stations_file) as src:
        stations_data = json.load(src)
    return stations_data

# %%
class DailyWeatherDATA:
    def __init__(self):
        self.url_daily_weather = 'https://www.bom.gov.au/climate/dwo/{year_month}/text/{bom_id}.{year_month}.csv'

        self.stations_infos = get_stations_infos()

    def get_available_cities(self):
        return list(self.stations_infos.keys())
    
    def get_station_info(self, city):
        station_info = self.stations_infos.get(city,None)

        if station_info is None:
            logger.error(f'Unknown city: {city}')
            raise ValueError("Unknown city")
        
        return station_info
    
    def get_station_date(self, city):
        station_info = self.get_station_info(city)        
        now = datetime.now(ZoneInfo(station_info['timezone']))
        return now
            
    def get_url(self,city, time='last'):
        station_info = self.get_station_info(city)
        
        if time=='last':
            now = self.get_station_date(city)
            date_month = now.strftime("%Y%m")
        elif len(time)==6 and time[0:2]=='20':
            date_month = time
        else:
            logger.error(f'Not valid time, expected YYYYMM format but got {time}')
            raise ValueError(f'{time}')
        
        return self.url_daily_weather.format(year_month=date_month, bom_id=station_info['bom_id'])
    
    def get_report(self,csv_url, out_dir):
        headers = {
                "User-Agent": "Mozilla/5.0"
            }
        
        temp_filename_split = osp.basename(csv_url).split('.')
        out_path = osp.join(out_dir, f'{temp_filename_split[0]}_{temp_filename_split[1]}.{temp_filename_split[2]}')

        if not osp.exists(out_path):
            logger.info('Download weather report')
            response = requests.get(csv_url, headers=headers)
            response.raise_for_status()

            with open(out_path, "wb") as f:
                f.write(response.content)

        return out_path

    def cleaning_report(self,in_file, out_dir):
        raw_cols = ['Date', 'Minimum temperature (°C)', 'Maximum temperature (°C)',
                    'Rainfall (mm)', 'Evaporation (mm)', 'Sunshine (hours)',
                    'Direction of maximum wind gust ', 'Speed of maximum wind gust (km/h)',
                    'Time of maximum wind gust', '9am Temperature (°C)',
                    '9am relative humidity (%)', '9am cloud amount (oktas)',
                    '9am wind direction', '9am wind speed (km/h)', '9am MSL pressure (hPa)',
                    '3pm Temperature (°C)', '3pm relative humidity (%)',
                    '3pm cloud amount (oktas)', '3pm wind direction',
                    '3pm wind speed (km/h)', '3pm MSL pressure (hPa)']

        dict_cols = {
            'Date':'datetime', 'MinTemp': 'float32', 'MaxTemp': 'float32', 'Rainfall': 'float32', 'Evaporation': 'float32',
            'Sunshine': 'float32', 'WindGustDir': 'string', 'WindGustSpeed': 'float32', 'TimeMaxWindGust': 'string',
            'Temp9am': 'float32', 'Humidity9am': 'int16', 'Cloud9am': 'float32', 'WindDir9am': 'string',
            'WindSpeed9am': 'float32', 'Pressure9am': 'float32', 'Temp3pm': 'float32', 'Humidity3pm': 'int16',
            'Cloud3pm': 'float32', 'WindDir3pm': 'string', 'WindSpeed3pm': 'float32', 'Pressure3pm': 'float32'
            }
        dict_raintoday = {0: 'No', 1: 'Yes'}
        
        logger.info('Cleaning weather report')
        df = pd.read_csv(in_file, delimiter=',', skiprows=7, encoding='latin-1')
        df = df.drop(columns=df.columns[0])
        
        if all(df.columns == raw_cols):
            df.columns = dict_cols.keys()
            
        for col,dtype in dict_cols.items():
            if dtype in ('int16', 'float32'):
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                
            elif dtype=='datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
            else:
                df[col] = df[col].astype(dtype)
        
        df = df.replace(
            [pd.NA, None, "NaN", "nan", "NAN", "NA", "N/A", "", " ", "null", "None"],
            np.nan
        )
        df = df.drop(columns='TimeMaxWindGust')
        df['RainToday'] = df['Rainfall'].apply(lambda x: dict_raintoday.get(x>1, None))

        filename = osp.basename(in_file)
        out_file = osp.join(out_dir, f'clean_{filename}')     
        df.to_csv(out_file, sep=',', header=True, index=False)
        return out_file

# %%
def run(city, maxlag=3):
    raw_report_dir = osp.join(PROJECT_ROOT, 'raw_data')
    clean_report_dir = osp.join(PROJECT_ROOT, 'data')
    
    process = DailyWeatherDATA()

    station_date = process.get_station_date(city)
    station_lag_date = station_date - timedelta(days=maxlag)

    if station_date.strftime("%m") != station_lag_date.strftime("%m"):
        month_list = [station_date.strftime("%Y%m"), station_lag_date.strftime("%Y%m")]
    else:
        month_list = [station_date.strftime("%Y%m")]
    
    list_clean_report = []
    for yearmonth in month_list:
        logger.info(f'Process {yearmonth}')
        url = process.get_url(city, yearmonth)
        raw_file = process.get_report(url, raw_report_dir)
        list_clean_report.append(process.cleaning_report(raw_file, clean_report_dir))
    
    if len(list_clean_report) > 1:
        logger.info('Merge all reports')
        list_df = [pd.read_csv(i) for i in list_clean_report]
        merge_df = pd.concat(list_df)

        merge_df['Date'] = pd.to_datetime(merge_df['Date'], errors='coerce')
        merge_df = merge_df.sort_values(by='Date')
        merge_df.to_csv(list_clean_report[0], sep=',', header=True, index=False, mode='w')
    
    logger.info('Finished !')

# %%
if __name__ == "__main__":
    # Parse command line arguments
    kwargs = parser.parse_args()
    print("\n")
    print("get Daily Weather Report command line arguments: \n")
    print(json.dumps(vars(kwargs), indent=1)) # Pretty print dictionary
    print("\n")

    # Run
    run(**vars(kwargs))

# %% [markdown]
# # TEST

# %%
data_file = osp.join(PROJECT_ROOT,'data','weatherAUS.csv')
df = pd.read_csv(data_file)
np.unique(df['Location'])

# %%
raw_report_dir = osp.join(PROJECT_ROOT, 'raw_data')
clean_report_dir = osp.join(PROJECT_ROOT, 'data')
city = 'Canberra'
max_lag = 3

process = DailyWeatherDATA()

station_date = process.get_station_date(city)
station_lag_date = station_date - timedelta(days=max_lag)

if station_date.strftime("%m") != station_lag_date.strftime("%m"):
    month_list = [station_date.strftime("%Y%m"), station_lag_date.strftime("%Y%m")]
else:
    month_list = [station_date.strftime("%Y%m")]

list_clean_report = []
for yearmonth in month_list:
    logger.info(f'Process {yearmonth}')
    url = process.get_url(city, yearmonth)
    raw_file = process.get_report(url, raw_report_dir)
    list_clean_report.append(process.cleaning_report(raw_file, clean_report_dir))



# %%
df = pd.read_csv(list_clean_report[0])
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

df = df.sort_values(by='Date')
df.head()
# df = df.sort_values(by='Date')
# df.head()


