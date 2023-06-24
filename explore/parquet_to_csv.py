from ftplib import FTP
import pandas as pd


ftp_ip = '192.168.1.10'
ftp_usr = 'bindz'
ftp_pwd = '240202'

ftp = FTP(ftp_ip)
ftp.login(ftp_usr, ftp_pwd)

# Download file from FTP server
# filename = 'taxi+_zone_lookup.csv'
# with open(filename, 'wb') as file:
#     ftp.retrbinary('RETR ' + filename, file.write)
files = ftp.nlst()
for file in files:
    if file.endswith('.parquet'):
        with open(file, 'wb') as f:
            ftp.retrbinary('RETR' + file, f.write)

ftp.quit()

# Thay ten file parquet o day
# df = pd.read_parquet('data/parquet/parquet_example.parquet')
# df.to_csv('data/csv/parquet_example.csv', index=False)

