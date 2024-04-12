import os
import subprocess
import json

# Path to the directory containing the files
directory_path = '/home/kniti/Desktop/nas/airflow'

# List the files in the directory
files_list = os.listdir(directory_path)

# Convert the list of files to a string separated by newline characters
files_str = '\n'.join(files_list)
print(files_str)
# Construct the JSON payload with the files_list as a string
data = {'conf': {'files_list': files_str}}

# Convert the dictionary to JSON string
json_data = json.dumps(data)

# Trigger the Airflow DAG using cURL command
curl_command = [
    "/usr/bin/curl",
    "-X", "POST",
    "http://localhost:8080/api/v1/dags/print_list/dagRuns", 
    "-H", "Content-Type: application/json",
    "-H", "Accept: application/json",
    "-u", "airflow:airflow",
    "-d", json_data
]
subprocess.run(curl_command, check=True)

print("DAG triggered successfully.")