from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import json
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
import paramiko
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False  # Setting catchup to False
}

dag = DAG(
    'upload_task',
    default_args=default_args,
    description='A DAG to receive folder path from Streamlit',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

def process_folder_path(**kwargs):
    folder_path = kwargs['dag_run'].conf.get('folder_path')
    folder_path=str(folder_path)
    if '/home/joel' in folder_path:
        folder_path=folder_path.replace('/home/joel','/opt/airflow')
    elif '/media/joel' in folder_path:
        folder_path=folder_path.replace('/media/joel/','/opt/airflow/media')
    print(f"Received folder path: {folder_path}")

    total_files=0

    ssh_hook = SSHHook(ssh_conn_id='vishnu')
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ssh_hook.remote_host, username=ssh_hook.username, password=ssh_hook.password)
    scp_client = ssh_client.open_sftp()
    local_dir = folder_path
    remote_dir = '/home/kniti/Desktop/download'
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            total_files=total_files+1
            local_path = os.path.join(root, file)
            remote_path = os.path.join(remote_dir, file)
            print(f"{total_files} Transferring {local_path} to {remote_path}")
            scp_client.put(local_path, remote_path)
            
            

    print(f"{total_files} uploaded Successfully !!")
    scp_client.close()
    ssh_client.close()

task_process_folder_path = PythonOperator(
    task_id='process_folder_path',
    python_callable=process_folder_path,
    provide_context=True,
    dag=dag,
)

task_process_folder_path
