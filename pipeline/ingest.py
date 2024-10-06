import json
import os
import requests
import datetime as dt
import pipeline.utils as utils
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')
HOME_LAT = os.getenv('HOME_LAT')
HOME_LONG = os.getenv('HOME_LONG')

def _get_weather(date: str, lat: str, long: str, *, verbose: bool = False) -> object:
    dailyAggEndpoint = 'https://api.openweathermap.org/data/3.0/onecall/day_summary'
    units = 'metric'

    # Endpoint has weather data for a particular date from 1979-01-02 and long-term forecast for 1.5 years ahead
    url = f'{dailyAggEndpoint}?appid={API_KEY}&date={date}&lat={lat}&lon={long}&units={units}'
    
    r = requests.get(url)
    
    if r.status_code != 200:
        if verbose:
            print(f'Error: Could not fetch weather data for {date}\n{r.status_code} - {r.content}')
        return None
    
    if verbose:
        print(f'Sucessfully fetched weather data for {date}')
    
    return json.loads(r.text)

def _save_weather_json(data: object, iso_date: str, *, verbose: bool = False):
    file_name = utils.CONFIG['raw_file_prefix'] + iso_date + '.' + utils.CONFIG['raw_file_format']
        
    raw_file_path = os.path.join(utils.CONFIG['data_dir'], utils.CONFIG['raw_folder'])
    
    if not os.path.exists(raw_file_path):
        os.makedirs(raw_file_path)
        
    with open(os.path.join(raw_file_path, file_name), 'w') as f:
        f.write(json.dumps(data, indent=2))
        
    if verbose:
        print(f'Written {file_name} to {raw_file_path}')

def run_ingest(iso_start_date: str, iso_end_date: str, *, verbose: bool = False):
    try:
        dates_list = utils.iso_dates_in_period(iso_start_date, iso_end_date, verbose=verbose)
    except ValueError:
        print(f'Error: Dates must be in iso string format to ingest weather data')
        return None
    
    utils.download_raw_s3_to_local(verbose=verbose)
    
    downloaded_dates = utils.get_local_raw_data_dates()
    dates_to_process = [date_str for date_str in dates_list if date_str not in downloaded_dates]
    
    if verbose:
        print(f'Fetching data for {dates_to_process}')
    
    for iso_date in dates_to_process:
        daily_weather = _get_weather(iso_date, HOME_LAT, HOME_LONG, verbose=verbose)
        if daily_weather:
            _save_weather_json(daily_weather, iso_date, verbose=verbose)
            
    utils.push_raw_local_to_s3(verbose=verbose)