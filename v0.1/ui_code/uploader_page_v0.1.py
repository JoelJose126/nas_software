import streamlit as st
import tkinter as tk
from tkinter import filedialog
import shutil
import os
import subprocess
import psycopg2
import json
from datetime import datetime 
import time
import cv2
import base64


# Database connection
conn = psycopg2.connect(
    database="kniti",
    user="postgres",
    password="55555",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

global user_id

def create_annotation_details_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS annotation_details (
    "s_id" SERIAL Primary Key,
    "file_id" SERIAL,
    "file_name" VARCHAR(255) UNIQUE,
    "file_type" VARCHAR(255) NOT NULL,
    "user_id" BIGINT NOT NULL,
    "mill_name" VARCHAR(255) NOT NULL,
    "unit_name" INTEGER NOT NULL,
    "machine_brand" VARCHAR(255) NOT NULL,
    "machine_type" VARCHAR(255) NOT NULL,
    "machine_dia" INTEGER NOT NULL,
    "knit_type" VARCHAR(255) NOT NULL,
    "fabric_material" VARCHAR(255) NOT NULL,
    "counts" INTEGER NOT NULL,
    "deniers" INTEGER NOT NULL,
    "background" BOOLEAN NOT NULL,                
    "colours" VARCHAR(255) NOT NULL,
    "fabric_rolling_type" VARCHAR(255) NOT NULL,
    "machine_gauge" INTEGER NOT NULL,
    "needle_drop" BOOLEAN NOT NULL,
    "cam_type" VARCHAR(255) NOT NULL,
    "lens_type" VARCHAR(255) NOT NULL,
    "machine_id" INTEGER NOT NULL,
    "roll_id" INTEGER NOT NULL,
    "rotation" INTEGER NOT NULL,
    "angle" INTEGER NOT NULL,
    "time_stamp" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "validated" BOOLEAN NOT NULL,
    "shapes_data" JSON,  
    "version" VARCHAR(255)
);

        """)
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Error creating table: {e}")

def create_upload_logs_table():
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS upload_log (
    s_id  BIGINT NOT NULL,
    upload_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    time_stamp TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    file_type varchar(30) NOT NULL,
    no_files INTEGER NOT NULL,
    validated BOOLEAN NOT NULL);
        """)
        conn.commit()
        # st.success("Upload logs table created successfully.")
    except psycopg2.Error as e:
        st.error(f"Error creating upload logs table: {e}")

def create_login_logs_table():
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS login_logs (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                timestamp TIMESTAMP
            )
        """)
        conn.commit()
        # st.success("Login logs table created successfully.")
    except psycopg2.Error as e:
        st.error(f"Error creating login logs table: {e}")

def create_download_logs_table():
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS download_logs (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                mill_name VARCHAR(50),
                file_type VARCHAR(50),
                download_timestamp TIMESTAMP
            )
        """)
        conn.commit()
        # st.success("Download logs table created successfully.")
    except psycopg2.Error as e:
        st.error(f"Error creating download logs table: {e}")




# Function to add new user, password, and role


# Function to check login credentials and role
def login(username, password):
    try:

        cur.execute("SELECT password, user_id,role FROM users WHERE username = %s", (username,))
        stored_data = cur.fetchone()
        
        if stored_data and password == stored_data[0]:
            # Log the login attempt
            log_login(username)
            return True, stored_data[1], username  # Return True, role, and username if login is successful
    except psycopg2.Error as e:
        st.error(f"Error checking login credentials: {e}")
    return False, None, None

# Function to log login attempt with timestamp
def log_login(username):
    # Create login_logs table if it doesn't exist
    create_login_logs_table()
    try:
        cur.execute("INSERT INTO login_logs (username, timestamp) VALUES (%s, %s)", (username, datetime.now()))
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Error logging login attempt: {e}")


# Login page
def login_page():
    global user_id

    st.title("Login Page")
    username = st.text_input("Username", placeholder="Username")
    password = st.text_input("Password", type="password", placeholder="Password")
    if st.button("Login"):
        success, user_id, username = login(username, password)
        if success:
            st.success("Login successful!")
            st.session_state['logged_in'] = True
            st.session_state['user_id'] = user_id 
             # Store the role in the session state
            st.session_state['username'] = username 
        else:
            st.error("Invalid username or password. Please try again.")
def separate_and_upload_to_postgres(folder_path, file_type, dropdown_data, user_id):
    key_order = ['mill_name', 'machine_number_id', 'roll_id', 'rotation', 'cam', 'Date', 'Hours', 'mins', 'seconds', 'angle', 'time_stamp']
    files_uploaded=0
    process_bar =st.progress(0)
    placeholder=st.empty()
    progress=0
    total_files=0
    destination_directory = '/home/joel/Desktop/Processed_JSON'
    # Load dropdown data from dropdown_data.json
    try:
        with open("dropdown_data.json", "r") as dropdown_file:
            dropdown_data = json.load(dropdown_file)
    except FileNotFoundError:
        st.error("Dropdown data file 'dropdown_data.json' not found.")
        return

    try:
        first_file_name = None  # Variable to store the name of the first file
          # Counter for total JSON files

        # Count total JSON files in the folder
        for _, _, files in os.walk(destination_directory):
            for file_name in files:
                if file_name.endswith('.json'):
                    total_files += 1
        # files_uploaded=0
        # Create progress bar
        # process_bar_text = st.text('Uploading To DB') 
        # process_bar_text.text('Uploading to DB')
        # progress_bar = st.progress(0)

        # Iterate through the files in the folder
        for i, (root, _, files) in enumerate(os.walk(folder_path)):
            for file_name in files:
                if file_name.endswith('.json'):
                    if first_file_name is None:
                        first_file_name = file_name  # Store the name of the first file

                    file_path = os.path.join(root, file_name)
                    try:
                        with open(file_path, "r") as file:
                            json_data = json.load(file)
                    except (json.JSONDecodeError, FileNotFoundError) as e:
                        st.error(f"Error loading JSON data from file: {file_path}. Reason: {e}")
                        continue

                    # Extracting information from the file name
                    file_info = file_name.split('_')
                    if len(file_info) >= len(key_order):
                        
                        data_dict = dict(zip(key_order, file_info))

                        timestamp = f"{data_dict['Date']}{data_dict['Hours']}{data_dict['mins']}-{data_dict['seconds']}"
                        data_dict['time_stamp'] = datetime.strptime(timestamp, "%Y-%m-%d%H%M-%S-%f").strftime("%Y-%m-%d %H:%M:%S.%f")

                        data_dict.pop('Date', None)
                        data_dict.pop('Hours', None)
                        data_dict.pop('mins', None)
                        data_dict.pop('seconds', None)
                        data_dict = {**data_dict, 'angle': data_dict.pop('angle'), 'time_stamp': data_dict['time_stamp']}

                        data_dict['file_type'] = file_type
                        data_dict['file_name'] = file_name
                        data_dict['user_id'] = user_id  # Assuming a static user_id for demonstration purposes
                        data_dict.update(dropdown_data)

                        # Extract additional keys from json_data
                        if 'shapes' in json_data:
                            shapes_data = json_data['shapes']
                        else:
                            shapes_data = []
                        version = json_data.get('version', None)

                        # Update data_dict with additional keys
                        data_dict.update({
                            'shapes_data': json.dumps(shapes_data),  # Convert shapes_data to JSON string
                            'version': version
                        })
                        files_uploaded=files_uploaded+1
                        try:
                            cur.execute("""
                                INSERT INTO annotation_details (
                                    user_id, machine_id, file_name, mill_name, roll_id, rotation, time_stamp, angle, file_type, 
                                    unit_name, machine_brand, machine_type, machine_dia, knit_type, fabric_material, 
                                    counts, deniers, colours, fabric_rolling_type, background, machine_gauge, 
                                    needle_drop, cam_type, lens_type, validated, shapes_data, version
                                ) 
                                VALUES (
                                    %(user_id)s, %(machine_id)s, %(file_name)s, %(mill_name)s, %(roll_id)s, %(rotation)s, 
                                    %(time_stamp)s, %(angle)s, %(file_type)s, %(unit_name)s, %(machine_brand)s, 
                                    %(machine_type)s, %(machine_dia)s, %(knit_type)s, %(fabric_material)s, 
                                    %(counts)s, %(deniers)s, %(colours)s, %(fabric_rolling_type)s, %(background)s, 
                                    %(machine_gauge)s, %(needle_drop)s, %(cam_type)s, %(lens_type)s, %(validated)s, 
                                    %(shapes_data)s, %(version)s
                                )
                                RETURNING s_id
                            """, data_dict)
                            sid = cur.fetchone()[0]  # Fetch the s_id of the inserted record
                            conn.commit()
                            
                        except (Exception, psycopg2.DatabaseError) as error:

                            if 'current transaction is aborted, commands ignored until end of transaction block' in error:
                                
                                files_uploaded=files_uploaded-1
                                
                                os.remove(f"{destination_directory/file_name}")
                                continue
                                
                            elif 'duplicate key value violates unique constraint'  in error:
                                
                                
                                files_uploaded=files_uploaded-1
                                os.remove(f"{destination_directory/file_name}") 
                                
                                continue
                            elif "UniqueViolation" in error:
                                
                                
                                files_uploaded=files_uploaded-1
                                os.remove(f"{destination_directory/file_name}")
                                continue
                            else :
                                
                                
                                files_uploaded=files_uploaded-1
                                os.remove(f"{destination_directory/file_name}")
                                st.error(f"Error uploading data from file: {file_path}. Reason: {error}")

                   
                        
                        progress=int(((files_uploaded) / total_files)*100)
                        placeholder.text(f"Uploading To DB !  Progress :  {progress}")
                        process_bar.progress(progress)  
        process_bar(1.0)   
        process_bar.empty()         
        placeholder.empty()
                        
        st.write(files_uploaded," : Entries populated in DB ")
        # progress_bar.progress(1.0)  
        # progress_bar.empty()              

        # Insert a record into the upload_log table using the s_id of the first file uploaded
        try:
            ts = datetime.now()
            # Fetch the s_id of the first file from the database
            cur.execute("SELECT s_id FROM annotation_details WHERE file_name = %s", (first_file_name,))
            first_file_row = cur.fetchone()  # Fetch the first row of the result
            if first_file_row:
                first_file_sid = first_file_row[0]  # Fetch the s_id from the first row
                # Insert a record into the upload_log table using the s_id of the first file
                try:
                    ts = datetime.now()
                    cur.execute("""
                        INSERT INTO upload_log (s_id, user_id, time_stamp,file_type,no_files, validated) 
                        VALUES (%s, %s, %s,%s, %s, %s)
                    """, (first_file_sid, user_id, ts,file_type, files_uploaded, dropdown_data['validated']))
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    
                    pass

                    # st.error(f"Error inserting record into upload_log table. Reason: {error}")
            elif "'UniqueViolation'" in error:
                
                pass
            else:
                st.error("No s_id found for the first file in the database.")
        except (Exception, psycopg2.DatabaseError) as error:
            
            pass
            # st.error(f"Error inserting record into upload_log table. Reason: {error}")
    except (Exception, psycopg2.DatabaseError) as error:
        
        pass
        # st.error(f"Error inserting record into upload_log table. Reason: {error}")
    return files_uploaded,total_files




                        
def append_json_to_files(folder_path, dropdown_data, file_type):
    # Function to append JSON data to the end of every JSON file in the folder based on file type
    files_processed=0
    placeholder = st.empty()
    total_files=0
    process_bar=st.progress(0)
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            total_files=total_files+1
    
    
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                try:
                    # Load JSON data from the file
                    with open(file_path, "r") as file:
                        existing_data = json.load(file)

                    # Update the existing data with the loaded JSON data
                    existing_data.update(dropdown_data)

                    # Move the file pointer to the beginning of the file
                    with open(file_path, "w") as file:
                        # Write the updated JSON data back to the file
                        json.dump(existing_data, file, indent=4)

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {file_path}: {e}")
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
            files_processed=files_processed+1
            progress=(int((files_processed/total_files)*100))
            placeholder.text(f"Appending JSON Form : {progress} %")
            process_bar.progress(progress)
    
    process_bar.empty()
    placeholder.empty()        
    return total_files

def load_dropdown_data():
    with open("dropdown_data.json", "r") as file:
        dropdown_data = json.load(file)
    return dropdown_data


def decode_FDA(file_path, dropdown_data):
    decoded_directory = '/home/joel/Desktop/Decoded_JSON'
    try:
        # Read the image using OpenCV
        img = cv2.imread(file_path)
        if img is None:
            print()
            raise Exception("Unable to read image file")

        # Get image height and width
        img_height, img_width, _ = img.shape

        # Encode image data using base64
        _, encoded_data = cv2.imencode('.jpg', img)
        encoded_data_str = base64.b64encode(encoded_data).decode('utf-8')

        # Get initial image name
        img_name = os.path.basename(file_path)

        # Create dictionary to store image data
        img_dict = {
            "img_path": img_name,
            "img_height": img_height,
            "img_width": img_width,
            "img_data": encoded_data_str
        }

        # Update dictionary with dropdown data
        img_dict.update(dropdown_data)

        # Convert dictionary to JSON
        json_data = json.dumps(img_dict)

        # Create destination directory if it doesn't exist
        if not os.path.exists(decoded_directory):
            os.makedirs(decoded_directory)

        # Generate JSON file path
        json_file_path = os.path.join(decoded_directory, img_name.replace('jpg','json'))

        # Write JSON data to file
        with open(json_file_path, "w") as json_file:
            json_file.write(json_data)

        return json_file_path, img_height, img_width, encoded_data_str

    except Exception as e:
        print(f"Error decoding image file '{file_path}': {e}")
        return None, None, None, None

def process_fda_files(selected_folder_path, dropdown_data):
    
    total_files=0
    destination_directory = '/home/joel/Desktop/Processed_JSON'
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    for root, _, files in os.walk(selected_folder_path):
        for file_name in files:
            if file_name.endswith('.jpeg') or file_name.endswith('.jpg'):
                total_files=total_files+1
    st.write(f"{total_files}  files Found !!")

    processed_files = []
    files_processed=0
    placeholder = st.empty()
    
    # st.text("Processing! Please wait..... ")
    process_bar = st.progress(0)
    
    for root, _, files in os.walk(selected_folder_path):
        for file_name in files:
            if file_name.endswith('.jpeg') or file_name.endswith('.jpg'):
                file_path = os.path.join(root, file_name)

                decoded_file_path, img_height, img_width, encoded_data_str = decode_FDA(file_path, dropdown_data)  # Pass dropdown_data here

                if decoded_file_path:
                    img_name = os.path.basename(decoded_file_path)

                    # Additional data to add to FDA JSON files
                    additional_data = {
                        "img_path": img_name,
                        "img_height": img_height,
                        "img_width": img_width,
                        "img_data": encoded_data_str,
                        "shape": {}  
                    }

                    with open(decoded_file_path, "r+") as f:
                        data = json.load(f)
                        data.update(additional_data)
                        f.seek(0)
                        json.dump(data, f, indent=4)
                        f.truncate()

                    processed_files.append(decoded_file_path)
                    shutil.copy(decoded_file_path, destination_directory)
                    os.remove(decoded_file_path)
                    files_processed=files_processed+1
                    processed_files.append(decoded_file_path)
                    
            progress=(int((files_processed/total_files)*100))
            placeholder.text(f"FDA Processing Progress : {progress} %")
            process_bar.progress(progress)
    process_bar.progress(1.0)
    process_bar.empty()
    placeholder.empty()    
    return processed_files,total_files 
                          
def mdd_copy(selected_folder_path):
    destination_directory='/home/joel/Desktop/Processed_JSON'
    total_files=0
    for root, _, files in os.walk(selected_folder_path):
        for file_name in files:
            if file_name.endswith('.json'):
                total_files=total_files+1
                file_path = os.path.join(root, file_name)
                
    

    # Construct the full destination path
                destination_path = os.path.join(destination_directory, file_name)
    
                shutil.copy(file_path, destination_path)
    st.write(f"{total_files} No of files Found !!") 
    
    return total_files

    

def uploader_page():        
    st.title("Uploader Page")
    st.write("You are logged in and can now upload files.")
    files_uploaded=0
    def select_folder():
        root = tk.Tk()
        root.withdraw()
        folder_path = filedialog.askdirectory(master=root)
        root.destroy()
        return folder_path

    selected_folder_path = st.session_state.get("folder_path", None)
    folder_select_button = st.button("Browse")
    if folder_select_button:
        selected_folder_path = select_folder()
        st.session_state.folder_path = selected_folder_path

    print("Selected folder path:", selected_folder_path)
    
    if selected_folder_path: 
        st.write("Selected folder path:", selected_folder_path)
        mill_name = st.selectbox("Select Mill Name", ["Fukuhara", "KPR 1", "Uzbek"])

        filetype = st.radio("Select File Type", ["MDD", "FDA"])  
        destination_directory = "/home/joel/Desktop/Processed_JSON"
        if st.button("Upload"):
            if filetype == "MDD":
                file_type = "MDD"
                # st.write("Processing MDD files...")
                dropdown_data = load_dropdown_data()
                
                files_uploaded=0
                mdd_copy(selected_folder_path)
                tot=append_json_to_files(destination_directory, dropdown_data, file_type)  
                files_uploaded,total_files= separate_and_upload_to_postgres(selected_folder_path, file_type, dropdown_data, st.session_state.get('user_id'))   
                
                if files_uploaded!=total_files:
                    
                    st.info(f'{total_files-files_uploaded} Duplicate or Faulty  files Found . These are skipped')  
                files_uploaded=files_uploaded-1       
                if files_uploaded>0:           
                    Trigger()
                    
                    st.write(f"{files_uploaded } Files uploaded and copied successfully.")
                # Copy the processed files to the destination directory
                else:

                    st.info(f"{files_uploaded } Files uploaded .")



            elif filetype == "FDA":
                file_type = "FDA"
                files_uploaded=0

                dropdown_data = load_dropdown_data()
                p_files,total_files=process_fda_files(selected_folder_path,dropdown_data)
                append_json_to_files(destination_directory, dropdown_data, file_type)  
                
                files_uploaded,total_files= separate_and_upload_to_postgres(selected_folder_path, file_type, dropdown_data, st.session_state.get('user_id'))   
                
                if files_uploaded!=total_files:
                    
                    st.info(f'{(total_files)-files_uploaded} Duplicate files Found . These are skipped')  
                files_uploaded=files_uploaded-1 
                if files_uploaded>0:       
                    Trigger()
                    st.write(f"{files_uploaded } Files uploaded and copied successfully.")
                else:

                    st.info(f"{files_uploaded } Files uploaded .")

def Trigger():
        success = execute_curl_command_upload()
        if success:
            st.write("\n DAG triggered successfully.")
        else:
            st.write("Error: Failed to trigger DAG.")





        
dropdown_data = {
    "unit_name": ["1", "2", "3","UNKNOWN"],
    "machine_brand": ["pailang", "mayer", "terrot", "hongji", "santoni", "fukuhara", "hisar", "Jacquard","UNKNOWN"],
    "machine_type": ["single_jersey", "double_jersey", "Jacquard","UNKNOWN"],
    "machine_dia": ["28", "30", "32", "34","UNKNOWN"],
    "knit_type": ["plain", "rib", "interlock", "purl", "Jacquard","UNKNOWN"],
    "fabric_material": ["cotton", "cotton_polyester", "lycra_cotton", "lycra_modal_cotton", "viscose_cotton", "lycra_polyester", "lycra_tensile_cotton", "lycra_polyester_cotton","UNKNOWN"],
    "counts": ["16", "18", "20", "24", "28", "30", "32", "34", "36", "38", "40","UNKNOWN"],
    "deniers": ["16", "18", "20", "24", "28", "30", "32", "34", "36", "38", "40","UNKNOWN"],
    "colours": ["grey", "melange", "white", "black", "brown", "green", "blue", "pattern","UNKNOWN"],
    "fabric_rolling_type": ["tubular", "openwidth","UNKNOWN"],
    "background": ["true", "false","UNKNOWN"],
    "machine_gauge": ["18", "20", "22", "24", "26", "28", "30", "32", "34", "36", "38", "40", "42", "44", "46","UNKNOWN"],
    "needle_drop": ["true", "false","UNKNOWN"],
    "cam_type": ["voltcam", "blackcam", "picam","UNKNOWN"],
    "lens_type": ["5mm", "default","UNKNOWN"    ],
    "validated":["False","True","UNKNOWN"],
    "machine_id":["1","2","UNKNOWN"]
    
}

def json_converter_page():
    st.title("JSON Converter")
    st.write("Select values from the dropdown menus to generate a JSON object.")

    selected_values = {}
    for key, values in dropdown_data.items():
        selected_value = st.selectbox(f"Select {key}", values)
        selected_values[key] = selected_value

    if st.button("Generate JSON"):
        # Store selected values in a JSON file in the same directory
        json_filename = "dropdown_data.json"
        with open(json_filename, "w") as json_file:
            json.dump(selected_values, json_file)
        st.write("JSON object generated successfully.")
        st.write(f"JSON file '{json_filename}' saved in the same directory.")

# Function to execute the curl command to trigger the DAG
def execute_curl_command_upload():
    try:
        # Use the full path to curl if it's not in the system PATH
        subprocess.run(["/usr/bin/curl", "-X", "POST", "http://localhost:8080/api/v1/dags/scp_example/dagRuns", "-H", "Content-Type: application/json", "-H", "Accept: application/json", "-u", "airflow:airflow", "-d", '{"note":"streamlit run"}'], check=True)
        return True
    except subprocess.CalledProcessError as e:
        return False



def Trigger():
        success = execute_curl_command_upload()
        if success:
            st.write("\n DAG triggered successfully.")
        else:
            st.write("Error: Failed to trigger DAG.")

# Function to log upload event
def log_upload(username, mill_name, file_type):
    try:
        cur.execute("INSERT INTO upload_logs (username, mill_name, file_type, upload_timestamp) VALUES (%s, %s, %s, %s)",
                    (username, mill_name, file_type, datetime.now()))
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Error logging upload event: {e}")

# Function to log download event
def log_download(username, mill_name, file_type):
    try:
        cur.execute("INSERT INTO download_logs (username, mill_name, file_type, download_timestamp) VALUES (%s, %s, %s, %s)",
                    (username, mill_name, file_type, datetime.now()))
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Error logging download event: {e}")



def main():
    create_login_logs_table()
    create_upload_logs_table()
    create_download_logs_table()
    create_annotation_details_table(conn)

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("", ["Login", "Json Conventor","Select Folder",])

    if page == "Login":
        login_page()

    if page=="Json Conventor" :
        json_converter_page()   
    elif page == "Select Folder":
        if st.session_state.get('logged_in'):
            uploader_page()
        else:
            st.error("You must be an admin to add a user.")    

if __name__ == "__main__":
    main()
