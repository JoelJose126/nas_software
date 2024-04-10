# Airflow DAG (dag.py)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import XCom
from datetime import datetime

import os
import paramiko


def append_file_names_to_list(**context):
    file_list_str = context['dag_run'].conf['file_names']
    file_list = file_list_str.split(',')
    remote_host = '192.168.1.104'
    remote_directory = '/home/kniti/Desktop/download'
    local_directory = '/opt/airflow/download'
    
    ssh_hook = SSHHook(ssh_conn_id='vishnu')
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ssh_hook.remote_host, username=ssh_hook.username, password=ssh_hook.password)
    scp_client = ssh_client.open_sftp()
    total_file=0
    for file_name in file_list:
        
        scp_client.get(f'{remote_directory}/{file_name}',f'{local_directory}/{file_name}',)
        total_file=total_file+1
        print(f"{total_file} Transferring {remote_directory}/{file_name}  to {local_directory}/{file_name}")
    
    scp_client.close()
    ssh_client.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 5),
}

with DAG('download_3', default_args=default_args, schedule_interval=None) as dag:
    files_task = PythonOperator(
        task_id='files_task',
        python_callable=append_file_names_to_list,
        provide_context=True
    )

    # get_files_task = PythonOperator(
    #     task_id='get_files_from_remote',
    #     python_callable=get_files_from_remote,
    #     provide_context=True
    # )

files_task 
