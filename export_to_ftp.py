import sqlite3
import pandas as pd
import json
import ftplib
from datetime import datetime
from getpass import getpass

with open('config.json', 'r') as file:
    config = json.load(file)

db_path = config['db_path']
db_password = getpass(prompt="Enter database password: ")  
last_execution_timestamp = config['last_execution_timestamp']
ftp_server = config['ftp']['server']
ftp_username = config['ftp']['username']
ftp_password = getpass(prompt="Enter FTP password: ")  
ftp_port = config['ftp']['port']
csv_path = config['ftp']['csv_path']
destination_path = config['ftp']['destination_path']
conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, password=db_password)
cursor = conn.cursor()
query = f"""
SELECT field1, field2, field3, field4
FROM your_table
WHERE last_updated > '{last_execution_timestamp}'
"""
cursor.execute(query)
data = cursor.fetchall()
columns = ['field1', 'field2', 'field3', 'field4']
df = pd.DataFrame(data, columns=columns)
df.to_csv(csv_path, index=False)
ftp = ftplib.FTP()
ftp.connect(ftp_server, ftp_port)
ftp.login(ftp_username, ftp_password)

with open(csv_path, 'rb') as file:
    ftp.storbinary(f'STOR {destination_path}', file)

ftp.quit()
config['last_execution_timestamp'] = datetime.now().isoformat()
with open('config.json', 'w') as file:
    json.dump(config, file, indent=4)

print("Script executed successfully.")
