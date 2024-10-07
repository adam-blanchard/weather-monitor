import pandas as pd
import matplotlib.pyplot as plt
import pipeline.utils as utils

def plot_rainfall(df: pd.DataFrame):
    df.plot(kind='line', y='precipitation')
    plt.show()

def run_serve(*, verbose: bool = False):
    df = pd.read_parquet(f'{utils.CONFIG["data_dir"]}/{utils.CONFIG["staging_folder"]}/{utils.CONFIG["rainfall_staging_file_name"]}.parquet')
    sorted_df = df.sort_index()
    enhanced_df = sorted_df
    enhanced_df['year'] = pd.DatetimeIndex(enhanced_df.index).year
    enhanced_df['month'] = pd.DatetimeIndex(enhanced_df.index).month
    enhanced_df['day'] = pd.DatetimeIndex(enhanced_df.index).day
    # enhanced_df.plot(kind='bar', x=['year', 'month'], y='precipitation')
    # plt.show()
    
    