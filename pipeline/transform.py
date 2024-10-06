import os
import glob
import pipeline.utils as utils
import pandas as pd
from tqdm import tqdm

def _load_raw_data(*, verbose: bool = False) -> pd.DataFrame:
    if verbose:
        print('Loading raw json data into dataframe')
    raw_json_files = glob.glob(f'{utils.CONFIG["data_dir"]}/{utils.CONFIG["raw_folder"]}/*.{utils.CONFIG["raw_file_format"]}')
    combined_df = pd.DataFrame()
    for json_file in tqdm(raw_json_files):
        df = pd.read_json(json_file)
        combined_df = pd.concat([combined_df, df])
    return combined_df

def _stage_raw_data(df: pd.DataFrame, *, verbose: bool = False):
    if verbose:
        print('Staging raw data into parquet')
    file_name = f'{utils.CONFIG["weather_staging_file_name"]}.parquet'
    staging_file_path = os.path.join(utils.CONFIG['data_dir'], utils.CONFIG['staging_folder'])
    
    if not os.path.exists(staging_file_path):
        os.makedirs(staging_file_path)
    
    df.to_parquet(os.path.join(staging_file_path, file_name))
    
    if verbose:
        print(f'Written file {os.path.join(staging_file_path, file_name)}')
    

def _stage_rainfall_data(df: pd.DataFrame, *, verbose: bool = False):
    if verbose:
        print('Staging rainfall data into parquet')
    total_df = df.filter(like='total', axis=0)
    rainfall_df = total_df.filter(items=['lat', 'lon', 'date', 'precipitation'])
    reindexed_df = rainfall_df.set_index('date')
    
    file_name = f'{utils.CONFIG["rainfall_staging_file_name"]}.parquet'
    staging_file_path = os.path.join(utils.CONFIG['data_dir'], utils.CONFIG['staging_folder'])
    
    reindexed_df.to_parquet(os.path.join(staging_file_path, file_name))
    
    if verbose:
        print(f'Written file {os.path.join(staging_file_path, file_name)}')


def run_transform(*, verbose: bool = False):
    df = _load_raw_data(verbose=verbose)
    _stage_raw_data(df, verbose=verbose)
    _stage_rainfall_data(df, verbose=verbose)