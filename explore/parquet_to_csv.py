from ftplib import FTP
import pandas as pd
import os


ftp_ip = '192.168.1.10'
ftp_usr = 'bindz'
ftp_pwd = '240202'

ftp = FTP(ftp_ip)
ftp.login(ftp_usr, ftp_pwd)

# Download file from FTP server
# filename = 'taxi+_zone_lookup.csv'
# with open(filename, 'wb') as file:
#     ftp.retrbinary('RETR ' + filename, file.write)
filenames = ftp.nlst()
for filename in filenames:
    if filename.endswith('.parquet'):
        local_filename = os.path.join('/home/tb24/projects/Datalake-project/data', filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR ' + filename, file.write)

ftp.quit()

# Thay ten file parquet o day
# df = pd.read_parquet('data/parquet/parquet_example.parquet')
# df.to_csv('data/csv/parquet_example.csv', index=False)

