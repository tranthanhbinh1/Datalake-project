from ftplib import FTP
from config.default import DATA_PATH
import pandas as pd
import polars as pl
import os

ftp_ip = '192.168.1.10'
ftp_usr = 'bindz'
ftp_pwd = '240202'

ftp = FTP(ftp_ip)
ftp.login(ftp_usr, ftp_pwd)

filenames = ftp.nlst()
for filename in filenames:
    if filename.endswith('.parquet'):
        local_filename = os.path.join(DATA_PATH, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR ' + filename, file.write)

ftp.quit()

# Convert parquet to csv for spooldir
all_files = os.listdir(DATA_PATH)
parquet_files = []
for file in all_files:
    if file.endswith('.parquet'):
        parquet_files.append(file)
print(parquet_files)

for name in parquet_files:
    file_name = os.path.splitext(name)[0]
    df = pd.read_parquet(os.path.join(DATA_PATH,name))
    df.to_csv(os.path.join(DATA_PATH, f'{file_name}.csv'), index=False)

# using POLARS - demn its BLAZINGLY FAST
all_files = os.listdir(DATA_PATH)
parquet_files = []
for file in all_files:
    if file.endswith('.parquet'):
        parquet_files.append(file)
print(parquet_files)

for name in parquet_files:
    file_name = os.path.splitext(name)[0]
    df = pl.read_parquet(os.path.join(DATA_PATH,name))
    df.write_csv(os.path.join(DATA_PATH, f'{file_name}.csv'))