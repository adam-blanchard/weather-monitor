import json
import datetime as dt
import glob
import boto3
from tqdm import tqdm

CONFIG = {}
with open('config.json', 'r') as f:
    CONFIG = json.loads(f.read())

s3_client = boto3.client('s3')

def _get_local_raw_data() -> list[str]:
    raw_json_files = glob.glob(f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/*.{CONFIG["raw_file_format"]}')
    return sorted([file_name.removeprefix(f'{CONFIG["data_dir"]}/') for file_name in raw_json_files])

def _get_local_staging_files() -> list[str]:
    staging_files = glob.glob(f'{CONFIG["data_dir"]}/{CONFIG["staging_folder"]}/*')
    return sorted([file_name.removeprefix(f'{CONFIG["data_dir"]}/') for file_name in staging_files])

def _list_s3_bucket_items(bucket_name: str, *, item_prefix: str = None) -> list[object]:
    bucket_items = []
    if item_prefix:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=item_prefix)
        if 'Contents' not in response:
            print(f'No bucket items with prefix: {item_prefix}')
            return None
        bucket_items = response['Contents']
        while 'NextContinuationToken' in response:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=item_prefix, ContinuationToken=response['NextContinuationToken'])
            bucket_items += response['Contents']
    else:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        bucket_items = response['Contents']
        while 'NextContinuationToken' in response:
            response = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
            bucket_items += response['Contents']
    
    return bucket_items

def _get_raw_bucket_item_key_from_date(iso_date: str) -> str:
    return f'{CONFIG["raw_file_bucket_dir"]}/{CONFIG["raw_file_prefix"]}{iso_date}.{CONFIG["raw_file_format"]}'

def _get_raw_local_file_path_from_date(iso_date: str) -> str:
    return f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/{CONFIG["raw_file_prefix"]}{iso_date}.{CONFIG["raw_file_format"]}'

def valid_iso_date(iso_date) -> bool:
    try:
        dt.date.fromisoformat(iso_date)
    except ValueError:
        return False
    return True

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

def list_s3_buckets():
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def lookup_lat_lon(address: str) -> tuple[str, str]:
    """
    Take an address and return a tuple of latitude and longitude
    """
    raise NotImplementedError

def get_s3_raw_data_dates():
    raw_s3_objects = _list_s3_bucket_items(CONFIG["bucket"], item_prefix=f'{CONFIG["raw_file_bucket_dir"]}/')
    return [
        s3_object['Key']
        .removeprefix(f'{CONFIG["raw_file_bucket_dir"]}/{CONFIG["raw_file_prefix"]}')
        .removesuffix(f'.{CONFIG["raw_file_format"]}')
        for s3_object in raw_s3_objects
        if s3_object['Key'] != f'{CONFIG["raw_file_bucket_dir"]}/'
    ]  

def get_local_raw_data_dates():
    file_names = _get_local_raw_data()
    return [
        file_name
        .removeprefix(f'{CONFIG["raw_folder"]}/{CONFIG["raw_file_prefix"]}')
        .removesuffix(f'.{CONFIG["raw_file_format"]}')
        for file_name in file_names
    ]

def download_raw_s3_to_local(*, verbose: bool = False):
    """
    Compare s3 objects with local objects, and download any objects that are in s3 but not local.

    Args:
        verbose (bool, optional): Flag to print process outputs to stdout. Defaults to False.
    """
    s3_dates = get_s3_raw_data_dates()
    local_dates = get_local_raw_data_dates()
    objects_to_download = [item for item in s3_dates if item not in local_dates]
    
    if verbose:
        print(f'Raw objects in s3: {int(len(s3_dates)):,}')
        print(f'Raw objects in local storage: {int(len(local_dates)):,}')
        print(f'Objects in s3 but not in local storage: {int(len(objects_to_download)):,}')
    
    if len(objects_to_download) == 0:
        print('No files to download')
        return None

    for object_date in tqdm(objects_to_download):
        bucket_item = _get_raw_bucket_item_key_from_date(object_date)
        local_file = _get_raw_local_file_path_from_date(object_date)

        s3_client.download_file(CONFIG['bucket'], bucket_item, local_file)


def push_raw_local_to_s3(*, verbose: bool = False):
    s3_dates = get_s3_raw_data_dates()
    local_dates = get_local_raw_data_dates()
    objects_to_push = [item for item in local_dates if item not in s3_dates]
    
    if verbose:
        print(f'Raw objects in s3: {int(len(s3_dates)):,}')
        print(f'Raw objects in local storage: {int(len(local_dates)):,}')
        print(f'Objects in s3 but not in local storage: {int(len(objects_to_push)):,}')

    if len(objects_to_push) == 0:
        print('No files to push')
        return None

    for object_date in tqdm(objects_to_push):
        bucket_item = _get_raw_bucket_item_key_from_date(object_date)
        local_file = _get_raw_local_file_path_from_date(object_date)

        s3_client.upload_file(local_file, CONFIG['bucket'], bucket_item)

def identify_missing_dates(iso_start_date: str, iso_end_date: str) -> list[str]:
    dates_in_period = iso_dates_in_period(iso_start_date, iso_end_date)
    local_dates = get_local_raw_data_dates()
    return sorted([item for item in dates_in_period if item not in local_dates])

def get_s3_staging_files(*, verbose: bool = False):
    if verbose:
        print('Fetching staging files from s3')
    staging_files = _list_s3_bucket_items(CONFIG['bucket'], item_prefix=f'{CONFIG["staging_bucket_dir"]}/')
    return staging_files

def push_staging_local_to_s3(*, verbose: bool = False):
    if verbose:
        print('Pushing staging files to s3')
    staging_files = _get_local_staging_files()
    
    for staging_file in tqdm(staging_files):
        file_path = f'{CONFIG["data_dir"]}/{staging_file}'
        s3_client.upload_file(file_path, CONFIG['bucket'], staging_file)