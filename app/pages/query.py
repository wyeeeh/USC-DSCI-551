import streamlit as st
import pandas as pd
import numpy as np
import os
import datetime
import time
import mysql.connector
from mysql.connector import Error


# # -- data repo --
# data_dir = os.path.join(os.getcwd(), "data")
# data = "domestic_violence_calls.csv"

# # -- load data --
# df = pd.read_csv(os.path.join(data_dir,data))
# ## convert date & time format
# df['Date Rptd'] = pd.to_datetime(df['Date Rptd']).dt.strftime('%Y-%m-%d')
# df['DATE OCC'] = pd.to_datetime(df['DATE OCC']).dt.strftime('%Y-%m-%d')
# df['TIME OCC'] = df['TIME OCC'].astype(str).str.zfill(4)
# df['TIME OCC'] = pd.to_datetime(df['TIME OCC'], format='%H%M').dt.strftime('%H:%M')


# -- global variables --
mapping_keys = {
    "DR_NO": "Records Number",
    "Date Rptd": "Report Date",
    "DATE OCC": "Occur Date",
    "TIME OCC": "Occur Time",
    "AREA": "Area Code",
    "AREA NAME": "Area",
    "Rpt Dist No": "Report District Number",
    "Part 1-2": "Part 1-2",
    "Crm Cd": "Crime Code",
    "Crm Cd Desc": "Crime",
    "Mocodes": "Mocodes",
    "Vict Age": "Victim Age",
    "Vict Sex": "Victim Sex",
    "Vict Descent": "Victim Descent",
    "Premis Cd": "Premise Code",
    "Premis Desc": "Premise",
    "Weapon Used Cd": "Weapon Used Code",
    "Weapon Desc": "Weapon",
    "Status": "Status Code",
    "Status Desc": "Status",
    "Crm Cd 1": "Crime Code 1",
    "Crm Cd 2": "Crime Code 2",
    "Crm Cd 3": "Crime Code 3",
    "Crm Cd 4": "Crime Code 4",
    "LOCATION": "Location",
    "Cross Street": "Cross Street",
    "LAT": "Latitude",
    "LON": "Longitude"
}

# -- config functions --
# @st.cache_data

## query functions
db_config = pd.read_json(os.path.join(os.getcwd(),'key.json'), typ = 'series').to_dict()
host=db_config['host']
user=db_config['user']
password=db_config['password']

# st.write(db_config)
# st.write(host)
# st.write(user)
# st.write(password)

def create_database_connection(area_code, host_name, user_name, user_password):
    # Create a database connection.
    db_name = f"Crime_{area_code}"
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        return connection
    except Error as e:
        print(f"Failed to connect to {db_name}: {e}")
        return None

    
def execute_query(connection, query):
    # Execute a query and return results as a DataFrame.
    try:
        if connection is not None:
            data_frame = pd.read_sql(query, connection)
            connection.close()
            return data_frame
    except Error as e:
        print(f"Error '{e}' occurred")
        return pd.DataFrame()

def query_all_areas(query, host_name, user_name, user_password):
    # Execute a query across all 21 Crime databases and collect results into a DataFrame.
    all_results = pd.DataFrame()
    for area_code in range(1, 22):
        connection = create_database_connection(area_code, host_name, user_name, user_password)
        if connection is not None:
            result = execute_query(connection, query)
            # result['Area_Code'] = area_code  # Add an Area_Code column to distinguish results
            all_results = pd.concat([all_results, result], ignore_index=True)
            connection.close()
    return all_results

def query_data(query):
    query = query
    if not query.lower().strip().startswith('select'):
        return "Error: Only SELECT queries are allowed for security reasons."
        

    results = query_all_areas(query, host_name=host, user_name=user, user_password=password)
    if not results.empty:
        data = results
    else:
        print("No data returned or an error occurred.")
        data = results
    return data

# -- create pages --

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    scan_filter = st.page_link("pages/scan&filter.py", label="Data Explorer", icon="🔍")
    query = st.page_link("pages/query.py", label="Query Explorer", icon="⌨️")
    edit = st.page_link("pages/edit.py", label="Data Editor", icon="📝")
    

## title & about the data
## title & about the data
st.write("""
         
# Query Explorer
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present         
""")

## query data
st.write('### ⌨️ SQL Query')
user_query = st.text_area("**Write your query below and press `Ctrl + Enter` to run:**", 
                           value="SELECT *\n"
                           "FROM CrimeIncident\n"
                           "WHERE Area = 5\n"
                           "LIMIT 10",
                           height = 200,
                           placeholder="SELECT *\n"
                           "FROM CrimeIncident\n"
                           "WHERE Area = 5\n"
                           "LIMIT 10",
                           label_visibility="visible")

## query result
tab1, tab2 = st.tabs(["🔢 Data","📋 Query"])
with tab1:
    st.write('### Data Result')
    st.write(query_data(user_query))
with tab2:
    st.write('### Query Executed')
    st.code(f"""
    {user_query}
    """)