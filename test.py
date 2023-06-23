from ftplib import FTP
from kafka import KafkaProducer

ftp_ip = '192.168.1.10'
ftp_usr = 'bindz'
ftp_pwd = '240202'


ftp = FTP(ftp_ip)
ftp.login(ftp_usr, ftp_pwd)

# Download file from FTP server
filename = 'taxi+_zone_lookup.csv'
with open(filename, 'wb') as file:
    ftp.retrbinary('RETR ' + filename, file.write)

# Connect to Kafka
conf = {'bootstrap.servers': 'localhost:9092'}  # config
producer = KafkaProducer(conf)

# Publish the downloaded file to a Kafka topic
with open(filename, 'rb') as file:
    data = file.read()
    producer.produce('my-topic', value=data)

# Close the FTP connection and flush Kafka messages
ftp.quit()
# producer.flush()