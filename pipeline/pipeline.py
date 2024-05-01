from prefect import flow, task
from pathlib import Path
import pandas as pd
# import pyarrow
# import pyarrow.parquet as pq
from prefect_gcp.cloud_storage import GcsBucket



#from prefect_gcp.cloud_storage import GcsBucket, GcpCredentials
#from prefect_gcp.bigquery import bigquery_load_cloud_storage
# gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-bucket")

@task()
def extract_data(file_path: str) -> pd.DataFrame:
#def extract_data(file_path: Path):
    # dtypes = {
    #             'loc': pd.Int64Dtype(),
    #             'lat': float,
    #             'long': float,
    #             'so2': float,
    #             'no2': float,
    #             'co': float,
    #             'o3': float,
    #             'pm10': float,
    #             'pm2.5': float
    # }
    # parse_dates = ['dt']
    # df = pd.read_csv(file_path, delimiter = ',', dtype=dtypes)
    df = pd.read_csv(file_path, delimiter = ',')
    print(df.head(5))
    # print(df.dtypes)

    return df

# @task
# def upload_to_gcs_partitioned(data: pd.DataFrame, bucket_path: str) -> None:
#     #function to upload the dataframe to gcs

#     #create a PyArrow table so it can be partitioned when uploading the data
#     table = pa.Table.from_pandas(data)
#     gcs = pa.fs.GcsFileSystem()

#     pq.write_to_dataset(
#         table,
#         root_path=bucket_path,
#         partition_cols=['dt'],
#         filesystem=gcs
#     )

@task
def upload_to_gcs(data: pd.DataFrame, path: str, bucket_name: str) -> None:
    gcs_bucket = GcsBucket.load('seoul-air-quality')
    gcs_bucket.upload_from_dataframe(
        data,
        to_path=path,
        serialization_format='parquet')

@task
def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    return data.rename(columns={'pm2.5': 'pm2_5'})

@flow()
def seoul_air():
    file_path = "./seoul_air_data/seoul_air_1988_2021.csv"
    bucket_name = "seoul_air_quality_gcs"
    table_name = "seoul_air_quality"
    bucket_path = f'{table_name}'
    
    data = extract_data(file_path)
    new_data = clean_data(data)
    # upload_to_gcs_partitioned(data, bucket_path)
    
    upload_to_gcs(new_data, bucket_path, bucket_name)
    

if __name__ == "__main__":
    seoul_air()
    
