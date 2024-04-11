import streamlit as st
import psycopg2
import pandas as pd
import datetime as datetime
import subprocess
import json 
from tkinter import  Tk,filedialog
import os
# Global variables


# Function to establish database connection
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
            WHERE  status='Rejected'    
        """, (st.session_state['username'],))
        
        tasks = cursor.fetchall()
        # cursor.execute("""
        #     UPDATE task_assign set status='Viewed' where assigned_to=%s AND  status='Rejected'
        # """, (st.session_state['username'],))
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



def reassign_task_log(selected_task_details):
    try:
        file_names = []
        conn = get_connection()
        cur = conn.cursor()
        flag=None
        user_dict={}
        assign_to_list=[]
        users_query = "SELECT user_id, username FROM users"
        users = execute_query(users_query)
        user_options = [user[1] for user in users]
        for user_id, username in users:
            user_dict[username] = user_id
            assign_to_list.append(username)

            # Assign To dropdown
        assign_to = st.selectbox("Assign To", user_options)
        user_id = user_dict.get(assign_to)
        task_timestamp = datetime.datetime.now()
        # Deadline date and time selector
        deadline_date = st.date_input("Deadline Date", value=datetime.datetime.now())
        deadline_time = st.time_input("Deadline Time")

        priority_options = ["Low", "Medium", "High"]
        priority = st.selectbox("Priority", priority_options)
        comments = st.text_area("Task Description or comments.")
        # st.info(f"{assign_to},{deadline_date},{deadline_time},{priority,comments}")

        status="Re-Assigned"
        no_of_files=len(file_names)
        # Priority dropdown
        status="Re-Assigned"
        no_of_files=len(file_names)
        # Priority dropdown

        str=""
        if st.button("Re-Assign Task"):
            for i, task_row in selected_task_details.iterrows():
                task_id = task_row['Task ID']    
                st.session_state['temp_task_id']=task_id
            # st.write(task_id)    
            # Retrieve file names associated with the task_id
            cur.execute("""
                SELECT file_name,file_type FROM task_assign WHERE task_id = %s
            """, (task_id,))
            row,file_type = cur.fetchone()
            cur.execute("""
                UPDATE task_assign set status ='Re-Assigned' WHERE task_id = %s
            """, (task_id,))
            conn.commit()
            file_names = row
            lis=file_names.split(',')
            print(len(lis))
            
            file_names=json.dumps(file_names)
            
        

            # Assign button
        
            try:
                conn = get_connection()
                cur = conn.cursor()
        
                cur.execute("""
                        INSERT INTO task_assign (assigned_by, assigned_to, task_timestamp, deadline_date, deadline_time, priority, comments, no_of_files, file_name,status,file_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
                    """, (
                        st.session_state['username'], assign_to, task_timestamp, deadline_date, deadline_time, priority, comments, len(lis), file_names,'Assigned',file_type
                    ))

                conn.commit()
                st.success("Task Re-assigned successfully!")

            except Exception as e:
                st.error(f"Error Re-assigning task: {e}")
            finally:
                conn.close()
    except Exception  as e :
        st.info(e)

    

    

    
def main():
    st.set_page_config(layout="wide") 
    


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
        st.title(" Task Re-Assign Page ")
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
                
                    if 'selected_task_details' in st.session_state is not None:
                        st.subheader("Selected Task Details:")
                        st.write(st.session_state['selected_task_details'])

                        
                # Empty line for spacing
                st.write("---")
                
                # Empty download and upload buttons side by side
                

                
                
                    # if 'selected_task_details' in st.session_state:
                try:
                    reassign_task_log(st.session_state['selected_task_details'])
                except :
                    pass
    
                
    
main()