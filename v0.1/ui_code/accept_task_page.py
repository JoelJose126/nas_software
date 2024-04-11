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
            FROM task_assign where
            status like 'Uploaded'
        """, (st.session_state['username'],))
        
        tasks = cursor.fetchall()
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
            dbname="kniti",
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
                
                # # Insert download log into task_download_log table
                # timestamp = str(datetime.datetime.now())
                # cur.execute("""
                #     INSERT INTO task_download_log (
                #         task_id,
                #         assigned_to,
                #         no_of_files,
                #         download_timestamp
                #     ) VALUES (%s, %s, %s, %s)
                # """, (task_id, assigned_to, no_of_files, timestamp))
                conn.commit()
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

def accept_task_dag(file_names):
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
        "http://localhost:8080/api/v1/dags/accept_task_upload/dagRuns",
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


def accept_task_log(selected_task_details):
    temp_directory='/home/joel/Desktop/upload_temp'
    
    try:
        try:
            file_names = []
            conn = get_connection()
            cur = conn.cursor()
            flag=None
            for i, task_row in selected_task_details.iterrows():
                task_id = task_row['Task ID']    
                st.session_state['temp_task_id']=task_id
                
                # Retrieve file names associated with the task_id
                cur.execute("""
                    SELECT file_name FROM task_assign WHERE task_id = %s
                """, (task_id,))
                row = cur.fetchone()
                
                file_str = row[0]
                file_names = file_str.split(',')
                st.session_state['temp_file_names']=file_names
                for file_name in file_names:
                    file_path = os.path.join(temp_directory, file_name)
                    
                    try:
                        with open(file_path, "r") as file:
                            json_data = json.load(file)
                            
                            if 'shapes' in json_data:
                                shapes_data = json_data['shapes']
                                shapes_data = json.dumps(shapes_data)
                                
                                # Update annotation_details table
                                cur.execute("""
                                    UPDATE annotation_details 
                                    SET shapes_data = %s, validated = 'true' 
                                    WHERE file_name = %s
                                """, (shapes_data, file_name))
                                

                        
                    except Exception as e:
                        print(f"Error reading JSON data from file {file_name}: {e}")
                
                # Update task_assign table
                cur.execute("""
                    UPDATE task_assign 
                    SET status = 'Completed' 
                    WHERE task_id = %s
                """, (task_id,))

            conn.commit()  # Commit the changes to the database
            st.success("Task Accepted")
            accept_task_dag(st.session_state['temp_file_names'])
        except (Exception, psycopg2.DatabaseError) as error:
            st.error(error)
    except (Exception, psycopg2.DatabaseError) as error:
        st.error(error)


def reject_task_log(selected_task_details):
    file_names = []
    conn = get_connection()
    cur = conn.cursor()
    flag=None
    # user_dict={}
    # assign_to_list=[]
    # users_query = "SELECT user_id, username FROM users"
    # users = execute_query(users_query)
    # user_options = [user[1] for user in users]
    # for user_id, username in users:
    #     user_dict[username] = user_id
    #     assign_to_list.append(username)

    #     # Assign To dropdown
    # assign_to = st.selectbox("Assign To", user_options)
    # user_id = user_dict.get(assign_to)
    # task_timestamp = datetime.datetime.now()
    # # Deadline date and time selector
    # deadline_date = st.date_input("Deadline Date", value=datetime.datetime.now())
    # deadline_time = st.time_input("Deadline Time")

    # priority_options = ["Low", "Medium", "High"]
    # priority = st.selectbox("Priority", priority_options)
    # comments = st.text_area("Task Description or comments.")
    # st.info(f"{assign_to},{deadline_date},{deadline_time},{priority,comments}")

    status="Re-Assigned"
    no_of_files=len(file_names)
    # Priority dropdown


    
    for i, task_row in selected_task_details.iterrows():
        task_id = task_row['Task ID']    
        st.session_state['temp_task_id']=task_id
        
        # Retrieve file names associated with the task_id
        cur.execute("""
            SELECT file_name,file_type FROM task_assign WHERE task_id = %s
        """, (task_id,))
        row,file_type = cur.fetchone()
        
        file_str = row[0]
        file_names = file_str.split(',')
        st.session_state['temp_file_names']=file_names
        file_str=json.dumps(file_names) 
    


        # Assign button
    # if st.button("Re-Assign Task"):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
                                UPDATE task_assign 
                                SET status='Rejected'
                                WHERE task_id = %s
                            """, (task_id,))
        # cur.execute("""
        #         INSERT INTO task_assign (assigned_by, assigned_to, task_timestamp, deadline_date, deadline_time, priority, comments, no_of_files, file_name,status,file_type)
        #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
        #     """, (
        #         st.session_state['username'], assign_to, task_timestamp, deadline_date, deadline_time, priority, comments, no_of_files, file_str,status,file_type
        #     ))

        conn.commit()
        st.success("Task Rejected successfully!")

    except Exception as e:
        st.error(f"Error assigning task: {e}")
    finally:
        conn.close()
    
    
    
    
    

    

def main():
    st.set_page_config(layout="wide") 

    # create_task_details_logs_table()
    # create_task_upload_log()
    # create_task_download_log()

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
        st.title(" Task Validation Page ")
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
                download_button_col, accept_button_col,reject_button_col = st.columns(3) 
                
                with download_button_col:
                    if st.button("Download"):
                        if 'selected_task_details' in st.session_state:
                            task_download_log(st.session_state['selected_task_details'])
                        else:
                            st.error("No task selected.")
                with accept_button_col:
                    if st.button("Accept"):
                        if 'selected_task_details' in st.session_state:
                            accept_task_log(st.session_state['selected_task_details'])
                            st.success("Acceptd")

                with reject_button_col:
                    if st.button("Reject"):
                        # if 'selected_task_details' in st.session_state:
                            
                        reject_task_log(st.session_state['selected_task_details'])
                        st.info("Task Rejected")
                        

                # with upload_button_col:
                #     if st.button("Upload"):
                #         total_files=0
                #         upload_files=[]
                #         folder_path = open_folder_selector()  # Call function to open folder selector
                #         if folder_path:
                #             st.success(f"Selected Folder Path: {folder_path}")
                #             for i, (root, _, files) in enumerate(os.walk(folder_path)):
                #                 for file_name in files:
                #                     total_files=total_files+1
                #                     upload_files.append(file_name)
                #             if 'selected_task_details' in st.session_state:        
                #                 task_upload_log(total_files,st.session_state['selected_task_details'],upload_files,folder_path)
                #             else:
                #                     st.error("No task selected.")
                #         else:
                #             st.warning("No folder selected!")
                        

                            # send_to_airflow(folder_path)  # Call function to send folder path to Airflow DAG
        
            else:
                st.write("No tasks found.")
        else:
            st.error("Error fetching user ID.")


main()


