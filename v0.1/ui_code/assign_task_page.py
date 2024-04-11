import streamlit as st
import psycopg2
from datetime import datetime

# Global variables
assign_to_list = []  
user_dict = {} 
no_of_files = 0 

# Function to establish database connection
def get_connection():
    return psycopg2.connect(
        database="kniti",
        user="postgres",
        password="55555",
        host="localhost",
        port="5432"
    )

# Function to create task_assign table
def create_task_assign_table():
    conn = get_connection()
    with conn.cursor() as cur:
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS task_assign (
                    task_id SERIAL PRIMARY KEY,
                    assigned_by varchar(250),
                    task_timestamp timestamp  ,
                    assigned_to VARCHAR(250),
                    deadline_date DATE,
                    deadline_time TIME,
                    priority VARCHAR(250),
                    comments VARCHAR(250),
                    no_of_files bigint,
                    file_name TEXT,
                    status varchar(30),
                    file_type varchar(20)
                    
                        
                )
            """)
            conn.commit()
        except psycopg2.Error as e:
            st.error(f"Error creating task_assign table: {e}")

# Function to create login_logs table
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
    except psycopg2.Error as e:
        st.error(f"Error creating login_logs table: {e}")

# @st.cache(suppress_st_warning=True)
def execute_and_upload_query(query):
    conn = get_connection()
    cursor = conn.cursor()

    if query.strip().split()[0].upper() == "SELECT":
        try:
            cursor.execute(query)
        
            
            data = cursor.fetchall()
            
            num_records = len(data)
            
            conn.commit()
            cursor.close()
            conn.close()
            return data, num_records  
        
              # Return an empty list for non-SELECT queries
        except Exception as e:
            st.error(f"Error: {str(e)}")
            return None, 0 
    else:  
            st.error("Permission Denied !!Only SELECT queries are allowed. ")
            conn.commit()
            cursor.close()
            conn.close()
            return [], 0
# Function to log login
def log_login(username):
    try:
        conn = get_connection()
        cur = conn.cursor()
        timestamp = str(datetime.now())
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
    query = f"SELECT * FROM users WHERE username = '{username}' AND password = '{password}'"
    result, num_records = execute_and_upload_query(query)
    return num_records > 0

def assign_task_page(no_of_files,file_type):
    result = None
    data=st.session_state['data']
    file_names=[]
    print(st.session_state['data'])
    status='Assigned'
    if data is not None:
        file_names = list(map(lambda x: x[0], data))
    else:
        raise ValueError
    global assign_to_list, user_dict  # Access global variables
    st.title("Assign Task")
    users_query = "SELECT user_id, username FROM users"
    users, num_records = execute_and_upload_query(users_query)
    if num_records > 0:
        user_options = [user[1] for user in users]
        for user_id, username in users:
            user_dict[username] = user_id
            assign_to_list.append(username)

        # Assign To dropdown
        assign_to = st.selectbox("Assign To", user_options)
        user_id = user_dict.get(assign_to)
        task_timestamp = datetime.now()
        # Deadline date and time selector
        deadline_date = st.date_input("Deadline Date", value=datetime.now())
        deadline_time = st.time_input("Deadline Time")

        file_str = ','.join(file_names)
        # Priority dropdown
        priority_options = ["Low", "Medium", "High"]
        priority = st.selectbox("Priority", priority_options)

        comments = st.text_area("Task Description or comments.")

        # Assign button
        if st.button("Assign Task"):
            try:
                conn = get_connection()
                cur = conn.cursor()

                cur.execute("""
                        INSERT INTO task_assign (assigned_by, assigned_to, task_timestamp, deadline_date, deadline_time, priority, comments, no_of_files, file_name,status,file_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
                    """, (
                        st.session_state['username'], assign_to, task_timestamp, deadline_date, deadline_time, priority, comments, no_of_files, file_str,status,file_type
                    ))

                conn.commit()
                st.success("Task assigned successfully!")

            except Exception as e:
                st.error(f"Error assigning task: {e}")
            finally:
                if conn is not None:
                    conn.close()



def main():
    create_task_assign_table()
    create_login_logs_table()
    data=None
    st.title("Task Management")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        if check_login_credentials(username, password):
            st.sidebar.success("Logged in successfully!")
            st.session_state['username']=username
            log_login(username)
            st.session_state['logged_in'] = True
            users_query = "SELECT user_id, username FROM users"
            users, num = execute_and_upload_query(users_query)
            st.session_state['num_records']=num
            for user_id, username in users:
                user_dict[username] = user_id
                assign_to_list.append(username)
        elif not st.session_state.get('logged_in'):
            st.error("You must login first.")
        else:
            st.sidebar.error("Invalid username or password")
    if st.session_state.get('logged_in'):
        # Execute SQL Query
        st.subheader("Execute SQL Query")
        file_type=st.radio("File Type",["MDD","FDA"])
        default_text=f"SELECT file_name from annotation_details where file_type LIKE '%{file_type}%' "
        
        query = st.text_area("Enter your SQL query here:",value=default_text)
        
        # file_type=st.radio("File Type",["MDD","FDA"])
        if st.button("Execute"):
            if query.strip() == "":
                st.error("Please enter a SQL query.")
            else:
                data, no_of_files = execute_and_upload_query(query)
                st.session_state['no_of_files'] = no_of_files 
                st.session_state['data']=data 

        # Assign Task
        num=st.session_state.get('num_records')
        if 'no_of_files' in st.session_state and st.session_state['no_of_files']   > 0:
            st.write(f"Number of files: {st.session_state['no_of_files']}")
            assign_task_page(st.session_state['no_of_files'],file_type) 
          
        else :
            # st.write(f"Number of files: {st.session_state['no_of_files']}")
            st.info(" Fetch records to assign Task ")

main()