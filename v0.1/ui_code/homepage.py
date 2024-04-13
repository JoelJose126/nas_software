import streamlit as st
import psycopg2
from datetime import datetime
from admin_task_assign import admin_task_page_main
from reassign_task_page import reassign_task_page_main
from accept_task_page import  accept_task_page_main
from Register import register_main
from uploader_page_v2 import uploader_page_main
from user_task_page import user_task_page_main
from download_2 import download_main
from database import get_connection


# Database connection
conn = get_connection()

# Function to check user credentials in the database
def login(username, password):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT password, role FROM users WHERE username = %s", (username,))
            stored_data = cur.fetchone()

            if stored_data:
                stored_password, role = stored_data
                if password == stored_password:
                    return True, role  # Return True and the role if login is successful
    except psycopg2.Error as e:
        st.error(f"Error accessing database: {e}")
    return False, None  # Return False if login is unsuccessful

# Function to log login events to the database
def log_login(username):
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO login_logs (username, timestamp) VALUES (%s, %s)", (username, datetime.now()))
            conn.commit()

    except psycopg2.Error as e:
        st.error(f"Error logging login event to database: {e}")
    

def admin_page(username):

    page = st.sidebar.radio("", ["Upload Data","Download","Task_assign","Task Validation","User Register"])
    
    if page == "Task_assign":
        admin_task_page_main(username)
        
    elif page == "Upload Data":
        uploader_page_main(username)
    elif page== "Download":
        download_main()   
    elif page=="Task Validation":
        pages=st.sidebar.radio("",["Accept task","Reject task"])
        if pages=="Accept task":
            accept_task_page_main(username)

        elif pages=="Reject task": 
            reassign_task_page_main()
    elif page=="User Register":
        register_main()

def user_page():
    page = st.sidebar.radio("", ["Tasks", "Upload Data"])
    
    if page == "Tasks":
        user_task_page_main()
    elif page == "Upload Data":
        uploader_page_main()

def main():
    st.set_page_config(layout="wide")
    if not st.session_state.get('logged_in'):
        st.title("Login Page")
        username = st.text_input("Username", placeholder="Username")
        
        password = st.text_input("Password", type="password", placeholder="Password")
        st.session_state['username']=username
        if st.button("Login"):
            login_success, role = login(username, password)  
            if login_success:
                st.session_state['Username']=username
                st.success(f"Login successful! Welcome {username.capitalize()}.")
                st.session_state['logged_in'] = True
                st.session_state['role'] = role
                st.session_state['username'] = username  # Store the username in the session state
                log_login(username)
            else:
                st.error("Invalid username or password. Please try again.")
    else:
        role = st.session_state.get('role')
        st.sidebar.title("Navigation")
        if role == 'admin':
            admin_page(st.session_state['Username']) 
        else:
            user_page(st.session_state['Username'])

if __name__ == "__main__":
    main()
