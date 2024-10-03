import json
import boto3

BUCKET = 'ab-rainfall-proj'
RAW_BUCKET_DIR = '/weather'

s3_client = boto3.client('s3')

def get_config():
    config = {}
    with open('config.json', 'r') as f:
        config = json.loads(f.read())
    return config


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
    # TODO: Fix config self referencing
    bucket_items = list_s3_bucket_items()
    parsed_bucket_items = [item.removeprefix(CONFIG['raw_file_prefix']).removesuffix(f'.{CONFIG["raw_file_format"]}') for item in bucket_items]
    missing_dates = [item for item in iso_date_list if item not in parsed_bucket_items]
    if verbose:
        print(f'Already have data for {len(iso_date_list) - len(missing_dates)} dates')
    return missing_dates