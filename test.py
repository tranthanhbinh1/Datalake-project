from ftplib import FTP
from kafka import KafkaProducer
from utils.utils import cdc_ftp
import time

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
kafka_topic = 'my-topic'
producer = KafkaProducer(conf)

# Scan for changes in the ftp server
previous_files = []
interval_seconds = 60  # Adjust the interval as needed

while True:
    current_files = ftp.nlst()
    new_files, deleted_files = cdc_ftp(previous_files, current_files)

    if new_files:
        for file in new_files:
            # Process or publish the new file to Kafka
            with ftp.open(file, 'rb') as ftp_file:
                file_content = ftp_file.read()
                producer.send(kafka_topic, file_content)

    if deleted_files:
        for file in deleted_files:
            # Handle the deleted file as needed
            print("Deleted file detected:", file)

    previous_files = current_files
    time.sleep(interval_seconds)


# Publish the downloaded file to a Kafka topic
# with open(filename, 'rb') as file:
#     data = file.read()
#     producer.produce('my-topic', value=data)

# Close the FTP connection and flush Kafka messages
# ftp.quit()
# producer.flush()