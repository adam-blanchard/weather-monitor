import pandas as pd
import plotly.express as px
import pipeline.utils as utils

RAINFALL_PARQUET = f'{utils.CONFIG["data_dir"]}/{utils.CONFIG["staging_folder"]}/{utils.CONFIG["rainfall_staging_file_name"]}.parquet'

def run_serve(*, verbose: bool = False):
    df = pd.read_parquet(RAINFALL_PARQUET)
    sorted_df = df.sort_index()
    fig = px.line(sorted_df, x=sorted_df.index, y="precipitation", title='Daily precipitation (mm)')
    fig.show()
    