import pandas as pd
import plotly.express as px
import pipeline.utils as utils

RAINFALL_PARQUET = f'{utils.CONFIG["data_dir"]}/{utils.CONFIG["staging_folder"]}/{utils.CONFIG["rainfall_staging_file_name"]}.parquet'

def add_calendar_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['year'] = pd.DatetimeIndex(df.index).year
    df['month'] = pd.DatetimeIndex(df.index).month
    df['year_month'] = pd.to_datetime(df.index).strftime('%Y-%m')
    df['day'] = pd.DatetimeIndex(df.index).day
    df['day_of_week'] = pd.DatetimeIndex(df.index).day_of_week
    df['day_name'] = pd.DatetimeIndex(df.index).day_name()
    df['week'] = df.index.isocalendar().week
    return df

def run_serve(*, verbose: bool = False):
    df = pd.read_parquet(RAINFALL_PARQUET)
    sorted_df = df.sort_index()
    enhanced_df = add_calendar_columns(sorted_df)
    year_month_df = (
        enhanced_df[['year_month', 'precipitation']]
        .groupby(['year_month']).sum()
    )
    rolling_df = (
        enhanced_df[['precipitation']]
        .rolling(7)
        .sum()
    )
    print(rolling_df.head())
    fig = px.line(rolling_df, x=rolling_df.index, y="precipitation", title='Rolling 7-day precipitation (mm)')
    fig.show()
    