from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False,  # Set catchup to False
}


def remove():
    local_dir = '/opt/airflow/processed'
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            os.remove(local_path)

def scp_files():
    total_files=0
    import paramiko
    ssh_hook = SSHHook(ssh_conn_id='vishnu')
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ssh_hook.remote_host, username=ssh_hook.username, password=ssh_hook.password)
    scp_client = ssh_client.open_sftp()
    local_dir = '/opt/airflow/processed'
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

dag = DAG(
    'scp_send_2',
    default_args=default_args,
    description='An example DAG to demonstrate SCP operation using SSHOperator',
    schedule_interval=None,catchup=False
)

start_task = DummyOperator(task_id='start_task', dag=dag)

scp_task = PythonOperator(
    task_id='scp_task',
    python_callable=scp_files,
    dag=dag,
)

delete_task= PythonOperator(
    task_id='delete_task',
    python_callable=remove,
    dag=dag,
)

start_task >> scp_task>>delete_task
