from ftplib import FTP

# Scan for changes in the ftp server
def cdc_ftp(previouse_file, current_file):
    new_files = [file for file in current_file if file not in previouse_file]
    deleted_files = [file for file in previouse_file if file not in current_file]
    return new_files, deleted_files