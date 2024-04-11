from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import shutil
import os

def copy_files(source_dir, destination_dir):
    total_files=0
    files = os.listdir(source_dir)
    for file in files:
        total_files=total_files+1
        source_path = os.path.join(source_dir, file)
        destination_path = os.path.join(destination_dir, file)
        shutil.copy(source_path, destination_path)
        print(f"Index : {total_files} Copied {source_path} to {destination_path}")

# Define the default arguments for the DAG  
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 10),
    'retries': 1
}

# Define the DAG
dag = DAG(
    'user_upload_temp',
    default_args=default_args,
    description='A DAG to copy files from one local directory to another',
    schedule_interval=None,catchup=False  # Set to None to manually trigger the DAG
)

# Define the task function to copy files
def copy_files_task(**kwargs):
    # Extract parameters from the DAG run's conf
    folder_path = kwargs['dag_run'].conf['folder_path']
    folder_path=str(folder_path)
    if '/home/joel' in folder_path:
        folder_path=folder_path.replace('/home/joel','/opt/airflow')
    elif '/media/joel' in folder_path:
        folder_path=folder_path.replace('/media/joel','/opt/airflow')
    destination_dir = '/opt/airflow/Desktop/upload_temp'  # Hardcoded destination directory

    # Call the copy_files function with the specified folder_path and destination_dir
    copy_files(folder_path, destination_dir)

# Define the PythonOperator to execute the copy_files_task
copy_files_task_op = PythonOperator(
    task_id='copy_files_task',
    python_callable=copy_files_task,
    provide_context=True,  # Allows the task to access context (e.g., parameters)
    dag=dag
)

# Set task dependencies (if any)
# This DAG consists of a single task
copy_files_task_op
