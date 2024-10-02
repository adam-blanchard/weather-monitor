import os
import json
import glob
import utils
import pandas as pd

CONFIG = utils.get_config()

def load_raw_data() -> pd.DataFrame:
    raw_json_files = glob.glob(f'{CONFIG["data_dir"]}/{CONFIG["raw_folder"]}/*.{CONFIG["raw_file_format"]}')
    combined_df = pd.DataFrame()
    for json_file in raw_json_files:
        df = pd.read_json(json_file)
        combined_df = pd.concat([combined_df, df])
    return combined_df

def stage_raw_data(df: pd.DataFrame):
    file_name = 'weather_stg.parquet'
    staging_file_path = os.path.join(CONFIG['data_dir'], CONFIG['staging_folder'])
    
    if not os.path.exists(staging_file_path):
        os.makedirs(staging_file_path)
    
    df.to_parquet(os.path.join(staging_file_path, file_name))

def stage_rainfall_data(df: pd.DataFrame):
    total_df = df.filter(like='total', axis=0)
    rainfall_df = total_df.filter(items=['lat', 'lon', 'date', 'precipitation'])
    reindexed_df = rainfall_df.set_index('date')
    
    file_name = 'total_daily_rainfall_stg.parquet'
    staging_file_path = os.path.join(CONFIG['data_dir'], CONFIG['staging_folder'])
    
    reindexed_df.to_parquet(os.path.join(staging_file_path, file_name))


if __name__ == '__main__':
    df = load_raw_data()
    stage_raw_data(df)
    
    weather_df = pd.read_parquet(os.path.join(CONFIG['data_dir'], CONFIG['staging_folder'], 'weather_stg.parquet'))
    stage_rainfall_data(weather_df)
    
    daily_rainfall_df = pd.read_parquet(os.path.join(CONFIG['data_dir'], CONFIG['staging_folder'], 'total_daily_rainfall_stg.parquet'))
    print(daily_rainfall_df.head())