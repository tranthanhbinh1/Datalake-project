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

def main():
    minio_host = "http://localhost:9000"

    s3 = boto3.resource('s3',
                        endpoint_url=minio_host,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    s3.Bucket('demo').upload_file("/home/tb24/projects/Datalake-project/data/yellow_tripdata_2023-01.csv",'yellow_taxi_data_Jan2023.csv')
    print('Successfully uploaded to bucket')


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)