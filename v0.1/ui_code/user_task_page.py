import streamlit as st
import psycopg2
import pandas as pd
import datetime as datetime
import subprocess
import json 
from tkinter import  Tk,filedialog
import os

def get_connection():
    return psycopg2.connect(
        database="kniti",
        user="postgres",
        password="55555",
        host="localhost",
        port="5432"
    )

def create_login_logs_table():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS login_logs (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                timestamp TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    except psycopg2.Error as e:
        st.error(f"Error creating login_logs table: {e}")
        
def create_task_upload_log():
    conn=get_connection()
    try:
        conn =get_connection()
        cur=conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_upload_log (
        task_id bigint PRIMARY KEY,
        user_id bigint,
        no_of_files INT,
        upload_timestamp timestamp
        )
        """)
        conn.commit()
        conn.close()
    except psycopg2.Error as e :
        st.error(f"Error creating task_upload_log table:{e}")

def create_task_details_logs_table():
    conn=get_connection()
    try:
        conn =get_connection()
        cur=conn.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS task_details (
                    task_id bigint PRIMARY KEY,
                    viewed varchar(30),
                    downloaded boolean,
                    uploaded boolean,
                    upload_status varchar(30),
                    accuracy INT
           )
        """)
        conn.commit()
        conn.close()
    except psycopg2.Error as e :
        st.error(f"Error creating task_details table:{e}")


def create_task_download_log():
    conn=get_connection()
    try:
        conn =get_connection()
        cur=conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_download_log (
        task_id bigint PRIMARY KEY ,
        assigned_to varchar(250) ,
        no_of_files INT ,
        download_timestamp timestamp
        )
        """) 
        conn.commit()
        conn.close()
    except psycopg2.Error as e :
        st.error(f"Error creating task_download_log table:{e}")

# Function to log login
def log_login(username):
    try:
        conn = get_connection()
        cur = conn.cursor()
        timestamp = str(datetime.datetime.now())
        cur.execute("""
            INSERT INTO login_logs (username, timestamp)
            VALUES (%s, %s)
        """, (username, timestamp))
        conn.commit()
        st.success("Login logged successfully!")
    except psycopg2.Error as e:
        st.error(f"Error logging login: {e}")
    finally:
        if conn is not None:
            conn.close()

# Function to check login credentials
def check_login_credentials(username, password):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = "SELECT * FROM users WHERE username = %s AND password = %s"
        cursor.execute(query, (username, password))
        user = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return user is not None
    except psycopg2.Error as e:
        st.error(f"Error checking login credentials: {e}")
        return False
    
def get_user_id(username):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
        user_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return user_id
    except psycopg2.Error as e:
        st.error(f"Error fetching user ID: {e}")
        return None


def get_user_tasks(user_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT task_id, assigned_by, priority, comments, file_type, deadline_date, deadline_time, no_of_files
            FROM task_assign 
            WHERE assigned_to = %s
        """, (st.session_state['username'],))
        
        tasks = cursor.fetchall()
        cursor.execute("""
            UPDATE task_assign set status='Viewed' where assigned_to=%s AND  status='Assigned'
        """, (st.session_state['username'],))
        conn.commit()
        cursor.close()
        conn.close()
        return tasks
    except psycopg2.Error as e:
        st.error(f"Error fetching user tasks: {e}")
        return []


def execute_query(query):
    try:
        # Establish connection to your PostgreSQL database
        conn = psycopg2.connect(
            dbname="knitting",
            user="postgres",
            password="55555",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        # st.write(records)
        conn.close()
        return records
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Function to download data
# Function to download data



def trigger_function(file_names):
    if not file_names:
        st.warning("No file names provided to trigger DAG.")
        return
    file_str = ','.join(file_names)
    # Construct the JSON payload with the file names as configuration
    payload = {
        "conf": {
            "file_names": file_names
        }
    }

    # Convert the payload to JSON string
    json_payload = json.dumps(payload)

    # Construct the curl command with the JSON payload
    curl_command = [
        "curl",
        "-X", "POST",
        "http://localhost:8080/api/v1/dags/download_3/dagRuns",
        "-H", "Content-Type: application/json",
        "-H", "Accept: application/json",
        "-u", "airflow:airflow",
        "-d", json_payload  
    ]

    try:
        subprocess.run(curl_command, check=True)
        st.write("DAG triggered successfully.")
    except subprocess.CalledProcessError as e:
        st.write(f"Error triggering DAG: {str(e)}")

def download_data(result):
    if not result:
        st.warning("No records to download.")
        pass

    # Extract file_ids from the result
    file_names = [row[0] for row in result]

    # Fetch file names from the database using file_ids
    

        # Display the list of file names
    if file_names:
        # st.write("List of File Names:")
        # st.write(file_names)
        
        # Call the trigger function if the list of file names is not empty
        trigger_function(file_names)  # Replace trigger_function() with your actual function call
    else:
        st.info("No file names found for the given file ids.")


def task_download_log(selected_task_details):
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        for i, task_row in selected_task_details.iterrows():
            task_id = task_row['Task ID']
            # Fetch relevant details from the task_assign table
            cur.execute("""
                SELECT assigned_to, no_of_files,file_name FROM task_assign WHERE task_id = %s 
            """, (task_id,))
            row = cur.fetchone()
            if row:
                assigned_to, no_of_files,task = row
                
                # Insert download log into task_download_log table
                timestamp = str(datetime.datetime.now())
                cur.execute("""
                    INSERT INTO task_download_log (
                        task_id,
                        assigned_to,
                        no_of_files,
                        download_timestamp
                    ) VALUES (%s, %s, %s, %s)
                """, (task_id, assigned_to, no_of_files, timestamp))
                cur.execute("""
                            UPDATE task_assign set status='Downloaded' where task_id=%s and status='Viewed'
                         """, (task_id,))
                conn.commit()
                conn.close()
                print(task)
                trigger_function(task)
                
                
                st.success(f"Downloaded files for task {task_id}")
            else:
                st.error(f"No details found for task {task_id}")
    except psycopg2.Error as e:
        e=str(e)

        if 'duplicate key value violates unique constraint' in e  :
            st.info("Download process  Already Initiated !! ")
        
            print(f"Error executing SQL query: {e}")
        else:
            st.error(f"Error executing SQL query: {e}")
    finally:
        if conn is not None:
            conn.close()

def upload_trigger(folder_path):
    payload = {
        "conf": {
            
            "folder_path":folder_path
        }}
    # Convert the payload to JSON string
    json_payload = json.dumps(payload)

    # Construct the curl command with the JSON payload
    curl_command = [
        "curl",
        "-X", "POST",
        "http://localhost:8080/api/v1/dags/user_upload_temp/dagRuns",
        "-H", "Content-Type: application/json",
        "-H", "Accept: application/json",
        "-u", "airflow:airflow",
        "-d", json_payload  
    ]

    try:
        subprocess.run(curl_command, check=True)
        st.write(" Upload DAG triggered successfully.")
    except subprocess.CalledProcessError as e:
        st.write(f"Error triggering DAG: {str(e)}")
    


def create_task_upload_log():
    conn=get_connection()
    try:
        conn =get_connection()
        cur=conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_upload_log (
        task_id bigint PRIMARY KEY,
        assigned_to varchar(250),
        no_of_files INT,
        file_name TEXT ,
        upload_timestamp timestamp
        )
        """)
        conn.commit()
        conn.close()
    except psycopg2.Error as e :
        st.error(f"Error creating task_upload_log table:{e}")

def open_folder_selector():
    root = Tk()
    root.withdraw()  # Hide the Tkinter window
    folder_path = filedialog.askdirectory()  # Open folder selector popup
    root.destroy()  # Destroy the Tkinter window after selection
    return folder_path
        
def task_upload_log(total_files,selected_task_details,file_names,folder_path):
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        for i, task_row in selected_task_details.iterrows():
            task_id = task_row['Task ID']
            print(task_id)
            # Fetch relevant details from the task_assign table
            cur.execute("""
                SELECT assigned_to FROM task_assign WHERE task_id = %s
            """, (task_id,))
            row = cur.fetchone()
            if row:
                assigned_to = row
                
                # Insert download log into task_download_log table
                timestamp = str(datetime.datetime.now())
                cur.execute("""
                    INSERT INTO task_upload_log (
                        task_id,
                        assigned_to,
                        no_of_files,
                        file_name,
                        upload_timestamp
                    ) VALUES (%s, %s, %s, %s,%s)
                """, (task_id, assigned_to, total_files,file_names, timestamp))
                cur.execute("""
                            UPDATE task_assign set status='Uploaded' where task_id=%s and  status='Downloaded'
                         """, (task_id,))
                conn.commit()
                conn.commit()
                conn.close()
                print("Db Populated")
                upload_trigger(folder_path)
                # trigger_function(task)
                
                
                st.success(f"Downloaded files for task {task_id}")
            else:
                st.error(f"No details found for task {task_id}")
    except psycopg2.Error as e:
        e=str(e)

        if 'duplicate key value violates unique constraint' in e  :
            st.info("Upload process  Already Initiated !! ")
        
            print(f"Error executing SQL query: {e}")
        else:
            st.error(f"Error executing SQL query: {e}")
    finally:
        if conn is not None:
            conn.close()


def main():
    st.set_page_config(layout="wide") 

    create_task_details_logs_table()
    create_task_upload_log()
    create_task_download_log()

    st.sidebar.title("Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    login_button = st.sidebar.button("Login")
    
    if login_button:
        if check_login_credentials(username, password):
            st.sidebar.success("Logged in successfully!")
            log_login(username)
            st.session_state['logged_in'] = True
            st.session_state['username'] = username  
        elif not st.session_state.get('logged_in'):
            st.sidebar.error("Invalid username or password")
    

    if st.session_state.get('logged_in'):
        st.title("User Task Page")
        user_id = get_user_id(st.session_state['username'])
        if user_id is not None:
            refresh_button = st.button("View task") 
            
            if refresh_button:
                tasks = get_user_tasks(user_id)
                st.session_state['tasks'] = tasks  
            
            else:
                tasks = st.session_state.get('tasks')
            
            if tasks:
                st.subheader("Your Tasks:")
                tasks_df = pd.DataFrame(tasks, columns=["Task ID", "Assigned By", "Priority", "Comments", "File Type", "Deadline Date", "Deadline Time", "No of Files"])
                
                # Convert deadline_time to match database format
                tasks_df["Deadline Time"] = tasks_df["Deadline Time"].apply(lambda x: x.strftime("%H:%M:%S"))
                task_container = st.container()
                
                with task_container:
                    select_col, df_col = st.columns([0.3, 4])  
                    
                    with select_col:
                        for i, row in tasks_df.iterrows():
                            if st.button(f"Task {row['Task ID']}", key=f"task_button_{row['Task ID']}"):
                                st.session_state['selected_task_details'] = tasks_df.iloc[i:i+1]  # Store the selected task details
                    
                    with df_col:
                        # Set the max-height using CSS
                        st.write(
                            f'<style>.dataframe .stScrollbar .simplebar-content-wrapper {{ max-height: 2000px !important; }}</style>',
                            unsafe_allow_html=True
                        )
                        st.table(tasks_df)
                
                    if 'selected_task_details' in st.session_state:
                        st.subheader("Selected Task Details:")
                        st.write(st.session_state['selected_task_details'])

                        
                # Empty line for spacing
                st.write("\n")
                
                # Empty download and upload buttons side by side
                download_button_col, upload_button_col = st.columns(2) 
                
                with download_button_col:
                    if st.button("Download"):
                        if 'selected_task_details' in st.session_state:
                            task_download_log(st.session_state['selected_task_details'])
                        else:
                            st.error("No task selected.")


                with upload_button_col:
                    if st.button("Upload"):
                        total_files=0
                        upload_files=[]
                        folder_path = open_folder_selector()  # Call function to open folder selector
                        if folder_path:
                            st.success(f"Selected Folder Path: {folder_path}")
                            for i, (root, _, files) in enumerate(os.walk(folder_path)):
                                for file_name in files:
                                    total_files=total_files+1
                                    upload_files.append(file_name)
                            if 'selected_task_details' in st.session_state:        
                                task_upload_log(total_files,st.session_state['selected_task_details'],upload_files,folder_path)
                            else:
                                    st.error("No task selected.")
                        else:
                            st.warning("No folder selected!")
                        

                            # send_to_airflow(folder_path)  # Call function to send folder path to Airflow DAG
        
            else:
                st.write("No tasks found.")
        else:
            st.error("Error fetching user ID.")


main()