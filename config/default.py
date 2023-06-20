import os

password = os.environ.get('NY_TAXI_PASSWORD')
POSTGRES_CONN_STRING = f'postgresql://datalake_user:{password}@localhost:5432/ny_taxi_data'

print(password)