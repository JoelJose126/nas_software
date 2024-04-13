import streamlit as st
import psycopg2
from database import get_connection

# Database connection
conn = get_connection()
cur = conn.cursor()

# Create users table if not exists
def create_users_table():
    try:
        # Create users table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE,
                password VARCHAR(50),
                role VARCHAR(20)
            );
         
        """)
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Error creating users table: {e}")

# Function to check if a user exists
def user_exists(username):
    cur.execute("SELECT username FROM users WHERE username = %s", (username,))
    return cur.fetchone() is not None

# Registration Function
def register(username, password, role):
    if user_exists(username):
        st.error("User already exists. Please choose a different username.")
        return False

    try:
        # Insert username, password, and role into the database
        cur.execute("""
            INSERT INTO users (username, password, role) VALUES (%s, %s, %s)
        """, (username, password, role))
        conn.commit()
        st.success("Registration successful!")
        return True
    except psycopg2.Error as e:
        # Handle database insertion error
        st.error(f"Error registering user: {e}")
        return False

def register_main():
    st.title("User Registration")
    create_users_table()

    username = st.text_input("Username", placeholder="Enter username")
    password = st.text_input("Password", type="password", placeholder="Enter password")
    role = st.selectbox("Role", ["admin", "user"])

    if st.button("Register"):
        if username and password:
            if register(username, password, role):
                st.write("You are now registered!")

if __name__ == "__main__":
    register_main()
