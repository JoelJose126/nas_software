import streamlit as st
import subprocess
import json
import tk
from tkinter import  Tk,filedialog

# Function to open Tkinter folder selector popup
def open_folder_selector():
    root = Tk()
    root.withdraw()  # Hide the Tkinter window
    folder_path = filedialog.askdirectory()  # Open folder selector popup
    root.destroy()  # Destroy the Tkinter window after selection
    return folder_path

# Streamlit UI
def main():
    st.title("Upload Folder Path to Airflow DAG")
    
    # Upload button
    if st.button("Upload Folder"):
        folder_path = open_folder_selector()  # Call function to open folder selector
        if folder_path:
            st.success(f"Selected Folder Path: {folder_path}")
            send_to_airflow(folder_path)  # Call function to send folder path to Airflow DAG
        else:
            st.warning("No folder selected!")

# Function to send folder path to Airflow DAG using curl
def send_to_airflow(folder_path):
    # Construct the JSON payload with the folder path
    payload = {
        "conf": {
            "folder_path": folder_path
        }
    }

    # Convert the payload to JSON string
    json_payload = json.dumps(payload)

    # Construct the curl command with the JSON payload
    curl_command = [
        "curl",
        "-X", "POST",
        "http://localhost:8080/api/v1/dags/upload_task/dagRuns",
        "-H", "Content-Type: application/json",
        "-H", "Accept: application/json",
        "-u", "airflow:airflow",
        "-d", json_payload  
    ]

    # Execute the curl command
    subprocess.run(curl_command)

main()