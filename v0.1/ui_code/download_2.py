import streamlit as st
import pandas as pd
import psycopg2
import json
import subprocess

# Function to execute SQL query
@st.cache_data
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
            "file_names": file_str
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
    

# Streamlit UI
st.title('Downloader Page')
# st.info()
default="SELECT file_name from annotation_details "
# Text area for SQL query input
query = st.text_area("Enter your SQL query:",value=default)

# Button to execute the query
if st.button("Execute"):
    if query=='':
        st.warning("Please enter an SQL Query first!")
    # Validate the query to allow only SELECT statements
    elif query.strip().upper().startswith("SELECT"):
        # Execute the query
        result = execute_query(query)
        if result:
            st.success(f"Query executed successfully. {len(result)} records fetched.")
        else:
            st.error("Error executing the query.")
    else:
        st.error("Only SELECT queries are allowed.")

# Button to initiate download
if st.button("Download"):
    # Fetch the result from the cached query result
    cached_result = execute_query(query)
    if cached_result and len(cached_result) > 0 and query:
        download_data(cached_result)
    else:
        st.info("Cannot initiate download! No records found or no query executed.")
