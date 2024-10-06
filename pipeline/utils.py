import json
import glob
import datetime as dt
import boto3

BUCKET = 'ab-rainfall-proj'
RAW_BUCKET_DIR = 'weather'

TEST_BUCKET = 'ab-demo-bucket'

CONFIG = {}
with open('config.json', 'r') as f:
    CONFIG = json.loads(f.read())

s3_client = boto3.client('s3')

def _get_local_raw_data() -> list[str]:
    raw_json_files = glob.glob(f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/*.{CONFIG["raw_file_format"]}')
    return sorted([file_name.removeprefix(f'{CONFIG["data_dir"]}/') for file_name in raw_json_files])

def _list_s3_bucket_items(bucket_name: str, *, item_prefix: str = None) -> list[object]:
    bucket_items = []
    if item_prefix:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=item_prefix)
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

def valid_iso_date(iso_date) -> bool:
    try:
        dt.date.fromisoformat(iso_date)
    except ValueError:
        return 0
    return 1

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

def download_raw_s3_to_local():
    # Compare s3 objects with local objects
    # Download any objects that are in s3 but not local
    s3_dates = get_s3_raw_data_dates()
    local_dates = get_local_raw_data_dates()
    objects_to_download = [item for item in s3_dates if item not in local_dates]
    print(f'Dates in s3: {len(s3_dates)}')
    print(f'Dates in local storage: {len(local_dates)}')
    print(f'Objects in s3 and not in local storage: {len(objects_to_download)}')

    for object_date in objects_to_download:
        bucket_item = f'{CONFIG["raw_file_bucket_dir"]}/{CONFIG["raw_file_prefix"]}{object_date}.{CONFIG["raw_file_format"]}'
        local_file = f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/{CONFIG["raw_file_prefix"]}{object_date}.{CONFIG["raw_file_format"]}'

        s3_client.download_file(BUCKET, bucket_item, local_file)

def sync_data_dir_with_s3():
    # TODO: Implement logic to sync local data dir with s3
    raise NotImplementedError