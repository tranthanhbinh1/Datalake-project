from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import os

load_dotenv()

LOCAL_FILE_PATH = os.environ.get('LOCAL_FILE_PATH')
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

minio_host = "http://localhost:9000"
# minio_client = Minio(minio_host, ACCESS_KEY, SECRET_KEY, secure=False)
# def main():
#     found = minio_client.bucket_exists("demo")
#     if not found:
#         minio_client.make_bucket("demo")
#     else:
#         print("Bucket already exists")

#     minio_client.fput_object("demo", "yellow_taxi_data_Jan2023.csv", "/home/tb24/projects/Datalake-project/data/yellow_tripdata_2023-01.csv",)
#     print('Successfully uploaded to bucket')


# if __name__ == "__main__":
#     try:
#         main()
#     except S3Error as exc:
#         print("error occurred.", exc)

s3 = boto3.resource('s3',
                    endpoint_url=minio_host,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

# upload a file from local file system '/home/john/piano.mp3' to bucket 'songs' with 'piano.mp3' as the object name.
s3.Bucket('demo').upload_file("/home/tb24/projects/Datalake-project/data/yellow_tripdata_2023-01.parquet",'yellow_taxi_data_Jan2023.parquet')
print('Successfully uploaded to bucket')