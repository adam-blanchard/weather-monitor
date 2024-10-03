import json
import glob
import datetime as dt
import boto3

BUCKET = 'ab-rainfall-proj'
RAW_BUCKET_DIR = '/weather'

CONFIG = {}
with open('config.json', 'r') as f:
    CONFIG = json.loads(f.read())

s3_client = boto3.client('s3')

def _get_local_raw_data() -> list[str]:
    raw_json_files = glob.glob(f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/*.{CONFIG["raw_file_format"]}')
    return sorted([file_name.removeprefix(f'{CONFIG["data_dir"]}/') for file_name in raw_json_files])

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

def sync_data_dir_with_s3():
    # TODO: Implement logic to sync local data dir with s3
    raise NotImplementedError

def list_s3_buckets():
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def list_s3_bucket_items() -> list[str]:
    """
    Get names for all items within the bucket
    """
    # TODO: Point at real bucket and handle pagination for more than 1,000 objects
    response = s3_client.list_objects_v2(Bucket='ab-demo-bucket')
    for item in response['Contents']:
        print(item['Key'])

def lookup_lat_lon(address: str) -> tuple[str, str]:
    """
    Take an address and return a tuple of latitude and longitude
    """
    raise NotImplementedError

def list_local_raw_data():
    raw_json_files = _get_local_raw_data()
    print(f'Locally downloaded raw datafiles: {len(raw_json_files)}')
    print(raw_json_files)

def get_local_raw_data_dates():
    file_names = _get_local_raw_data()
    return [file_name.removeprefix(f'{CONFIG["raw_folder"]}/{CONFIG["raw_file_prefix"]}').removesuffix(f'.{CONFIG["raw_file_format"]}') for file_name in file_names]