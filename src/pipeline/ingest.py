import datetime as dt
import json
import os
import requests
import boto3
import src.pipeline.utils as utils
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')
HOME_LAT = os.getenv('HOME_LAT')
HOME_LONG = os.getenv('HOME_LONG')

CONFIG = utils.get_config()

BUCKET = 'ab-rainfall-proj'
RAW_BUCKET_DIR = '/weather'

s3_client = boto3.client('s3')

def get_weather(date: str, lat: str, long: str, *, verbose: bool = False) -> object:
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

def iso_dates_in_period(start_date_str: str, end_date_str: str = None, *, verbose: bool = False) -> list[str]:
    if not end_date_str:
        end_date_str = start_date_str
    
    if start_date_str > end_date_str:
        start_date_str, end_date_str = end_date_str, start_date_str
    
    start_date = dt.date.fromisoformat(start_date_str)
    end_date = dt.date.fromisoformat(end_date_str)
    
    dateDiff = end_date - start_date

    output = []

    for x in range(dateDiff.days + 1):
        date = start_date + dt.timedelta(days=x)
        output.append(date.isoformat())
        
    if verbose:
        print(f'{len(output)} dates in range')    
    
    return output

def save_weather_json(data: object, iso_date: str, *, verbose: bool = False):
    file_name = CONFIG['raw_file_prefix'] + iso_date + '.' + CONFIG['raw_file_format']
        
    raw_file_path = os.path.join(CONFIG['data_dir'], CONFIG['raw_folder'])
    
    if not os.path.exists(raw_file_path):
        os.makedirs(raw_file_path)
        
    with open(os.path.join(raw_file_path, file_name), 'w') as f:
        f.write(json.dumps(data, indent=2))
        
    if verbose:
        print(f'Written {file_name} to {raw_file_path}')

def sync_data_dir_with_s3():
    # TODO: Implement logic to sync local data dir with s3
    pass

def list_s3_buckets():
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def list_s3_bucket_items() -> list[str]:
    """
    Get names for all items within the bucket
    """
    # return ['openweather_dailyagg_2024-01-01.json', 'openweather_dailyagg_2024-01-02.json', 'openweather_dailyagg_2024-01-03.json']
    response = s3_client.list_objects_v2(Bucket='ab-demo-bucket')
    for item in response['Contents']:
        print(item['Key'])
        

    
def check_data_exists_in_bucket(iso_date_list: list[str], *, verbose: bool = False) -> list[str]:
    """
    Checks if data has already been downloaded for a list of dates and returns a list of dates that we do not have data for
    """
    bucket_items = list_s3_bucket_items()
    parsed_bucket_items = [item.removeprefix(CONFIG['raw_file_prefix']).removesuffix(f'.{CONFIG["raw_file_format"]}') for item in bucket_items]
    missing_dates = [item for item in iso_date_list if item not in parsed_bucket_items]
    if verbose:
        print(f'Already have data for {len(iso_date_list) - len(missing_dates)} dates')
    return missing_dates

if __name__ == '__main__':
    start_date = ''
    end_date = ''
    print('Welcome to the weather monitor fetch service')
    while True:
        data = input('Please enter a start and end date in iso format seperated by a space: ')
        if data == 'exit':
            exit
        split_data = data.split(' ')
        if len(split_data) == 2:
            start_date = split_data[0]
            end_date = split_data[1]
        else:
            print('Input must be two iso dates seperated by a space. Please try again')
            continue
        print(f'Your chosen dates are\nstart date: {start_date}\nend date: {end_date}')
        input_again = input('Are you happy with these dates? (y/n) ')
        if input_again != 'y':
            continue
        proceed = input('Would you like to fetch all data within this range? (y/n) ')
        if proceed != 'y':
            exit
        else:
            break
    
    dates_list = iso_dates_in_period(start_date, end_date, verbose=True)
    
    # dates_to_fetch = check_data_exists_in_bucket(dates_list, verbose=True)
    # print(dates_to_fetch)
    
    for iso_date in dates_list:
        daily_weather = get_weather(iso_date, HOME_LAT, HOME_LONG, verbose=True)
        if daily_weather:
            save_weather_json(daily_weather, iso_date, verbose=True)
        
    # list_s3_buckets()
    
    # list_s3_bucket_items()