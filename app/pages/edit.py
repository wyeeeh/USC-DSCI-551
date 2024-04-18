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

def create_database_connection(area):
    try:
        # First connect to the MySQL server without specifying the database
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor()
        # Create a database name such as Crime_1
        db_name = f'Crime_{int(area)}'  
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        cursor.close()
        
        # Connect to the latest database
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_name,
            auth_plugin='mysql_native_password'
        )
        if conn.is_connected():
            print(f"Connected to database {db_name}")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None

def load_data():
    all_results = pd.DataFrame()
    for area_code in range(1, 22):    
        try:
            connection = create_database_connection(area_code)
            if connection is not None:      
                query = """
                SELECT ci.DR_NO, Date_Rptd, DATE_OCC, TIME_OCC, AREA, Rpt_Dist_No, Part_1_2, Mocodes, Status, Status_Desc, Vict_Age, Vict_Sex, Vict_Descent, Premis_Cd, Premis_Desc, Weapon_Used_Cd, Weapon_Desc, Cri_Cd1, Cri_Cd2, Cri_Cd3, Cri_Cd4, Loc, Cro_Street, Lat, Lon
                FROM CrimeIncident ci
                JOIN Victim v ON ci.DR_NO = v.DR_NO
                JOIN IncidentCrimeCode ic ON ci.DR_NO = ic.DR_NO
                JOIN PremiseWeapon pw ON ci.DR_NO = pw.DR_NO
                JOIN Location l ON ci.DR_NO = l.DR_NO;
                """
                data_frame = pd.read_sql(query, connection)
                connection.close()
            else:
                data_frame = pd.DataFrame()
        except Error as e:
            print(f"Error '{e}' occurred")
        all_results = pd.concat([all_results, data_frame], ignore_index=True)
    return all_results

def delete_record(data):
    """Deletes records from the corresponding databases and tables, ensuring no foreign key constraint fails."""
    if 'AREA' not in data:
        print("Error: AREA field is missing in the provided data.")
        return

    area = data['AREA']
    conn = create_database_connection(area)
    if conn is None:
        return  # If the connection fails, return

    cursor = conn.cursor()
    try:
        # Ensure data dictionary values are None if they are NaN (pandas handling)
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        # Delete records from dependent tables first
        # Delete a record from the Victim table
        cursor.execute("""
        DELETE FROM Victim WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Victim")

        # Delete a record from the PremiseWeapon table
        cursor.execute("""
        DELETE FROM PremiseWeapon WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from PremiseWeapon")

        # Delete a record from the IncidentCrimeCode table
        cursor.execute("""
        DELETE FROM IncidentCrimeCode WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from IncidentCrimeCode")

        # Delete a record from the Location table
        cursor.execute("""
        DELETE FROM Location WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Location")

        # Finally, delete the record from the CrimeIncident table
        cursor.execute("""
        DELETE FROM CrimeIncident WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from CrimeIncident")

        conn.commit()
    except Error as e:
        print(f"Error deleting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_record(data):
    """Insert records into the appropriate databases and tables"""
    if 'AREA' not in data:
        print("Error: AREA field is missing in the provided data.")
        return
    
    area = data['AREA']
    conn = create_database_connection(area)
    if conn is None:
        return  # If the connection fails, return

    cursor = conn.cursor()
    try:
        # Replace missing values with None
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        # Insert data into the CrimeIncident table
        cursor.execute("""
        INSERT INTO CrimeIncident (DR_NO, Date_Rptd, DATE_OCC, TIME_OCC, AREA, Rpt_Dist_No, Part_1_2, Mocodes, Status, Status_Desc)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Date_Rptd=VALUES(Date_Rptd), DATE_OCC=VALUES(DATE_OCC), TIME_OCC=VALUES(TIME_OCC), Part_1_2=VALUES(Part_1_2), Mocodes=VALUES(Mocodes), Status=VALUES(Status), Status_Desc=VALUES(Status_Desc);
        """, (data.get('DR_NO'), data.get('Date Rptd'), data.get('DATE OCC'), data.get('TIME OCC'), data.get('AREA'), data.get('Rpt Dist No'), data.get('Part 1-2'), data.get('Mocodes'), data.get('Status'), data.get('Status Desc')))

        # Insert data into the Area table
        cursor.execute("""
        INSERT INTO Area (AREA, AREA_NAME)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE AREA=VALUES(AREA);
        """, (data.get('AREA'), data.get('AREA NAME')))

        # Insert data into the Victim table
        cursor.execute("""
        INSERT INTO Victim (DR_NO, Vict_Age, Vict_Sex, Vict_Descent)
        VALUES (%s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('Vict Age'), data.get('Vict Sex'), data.get('Vict Descent')))
            
        # Insert data into the PremiseWeapon table
        cursor.execute("""
        INSERT INTO PremiseWeapon (DR_NO, Premis_Cd, Premis_Desc, Weapon_Used_Cd, Weapon_Desc)
        VALUES (%s, %s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('Premis Cd'), data.get('Premis Desc'), data.get('Weapon Used Cd'), data.get('Weapon Desc')))
       
         
        # Insert data into the IncidentCrimeCode table
        cursor.execute("""
        INSERT INTO IncidentCrimeCode (DR_NO, Crime_Code, Cri_Cd1, Cri_Cd2, Cri_Cd3, Cri_Cd4)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data.get('DR_NO'), data.get('Crm Cd'), data.get('Crm Cd 1'), data.get('Crm Cd 2'), data.get('Crm Cd 3'), data.get('Crm Cd 4')))


        # Insert data into the Location table
        cursor.execute("""
        INSERT INTO Location (DR_NO, Loc, Cro_Street, Lat, Lon)
        VALUES (%s, %s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('LOCATION'), data.get('Cross Street'), data.get('LAT'), data.get('LON')))

        conn.commit()
        print("Record inserted successfully")
    except Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


field_to_tables = {
    'Date_Rptd': ['CrimeIncident'],
    'DATE_OCC': ['CrimeIncident'],
    'TIME_OCC': ['CrimeIncident'],
    'AREA': ['CrimeIncident'],
    'Rpt Dist No': ['CrimeIncident'],
    'Part 1-2': ['CrimeIncident'],
    'Mocodes': ['CrimeIncident'],
    'Status': ['CrimeIncident'],
    'Status Desc': ['CrimeIncident'],
    'AREA': ['Area'],
    'AREA_NAME': ['Area'],
    'Vict_Age': ['Victim'],
    'Vict_Sex': ['Victim'],
    'Vict_Descent': ['Victim'],
    'Premis Cd': ['PremiseWeapon'],
    'Premis Desc': ['PremiseWeapon'],
    'Weapon Used Cd': ['PremiseWeapon'],
    'Weapon Desc': ['PremiseWeapon'],
    'Vict_Sex': ['PremiseWeapon'],
    'Crime_Cod': ['IncidentCrimeCode'], 
    'Cri_Cd1': ['IncidentCrimeCode'], 
    'Cri_Cd2': ['IncidentCrimeCode'], 
    'Cri_Cd3': ['IncidentCrimeCode'], 
    'Cri_Cd4': ['IncidentCrimeCode'],    
    'Loc': ['Location'],   
    'Cro_Street': ['Location'],   
    'Lat': ['Location'],   
    'Lon': ['Location'],
       
    'DR_NO': ['CrimeIncident','Area','Victim','PremiseWeapon','IncidentCrimeCode']   
    # 添加更多字段和表的映射
}


        
def update_records(update_data):
    area = update_data['AREA']
    primary_key = update_data['primary_key']  # 主键名
    primary_key_value = update_data['primary_key_value']  # 主键值
    db_name = f'Crime_{int(area)}'
    
    conn = create_database_connection(area)
    if conn is None:
        return  # If the connection fails, return

    cursor = conn.cursor()
    try:
        # 遍历更新数据中的字段，更新所有相关的表
        for field, value in update_data['updates'].items():
            if field in field_to_tables:
                for table in field_to_tables[field]:
                    sql = f"UPDATE {table} SET {field} = %s WHERE {primary_key} = %s"
                    cursor.execute(sql, (value, primary_key_value))
                    conn.commit()
                    print(f"Updated {field} in {table} for {primary_key} = {primary_key_value}")
        
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# -- create pages --

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    scan_filter = st.page_link("pages/scan&filter.py", label="Data Explorer", icon="🔍")
    query = st.page_link("pages/query.py", label="Query Explorer", icon="⌨️")
    edit = st.page_link("pages/edit.py", label="Data Editor", icon="📝")
    

## title & about the data
st.write("""
         
# Data Editor
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present         
""")

## load data
df = load_data()
df_cache = df.copy()

st.write('### 📝 Edit Data')

# st.data_editor(df, key="my_key", disabled=("DR_NO", "AREA"), num_rows="dynamic") # 👈 Set a key
st.data_editor(df, key="my_key", num_rows="dynamic") # 👈 Set a key
# st.write("Here's the value in Session State:")

st.write("**Change Preview:**")
st.write(st.session_state["my_key"]) # 👈 Show the value in Session State

# if st.session_state.get('clear'):
#     st.session_state['my_key'] = ''

# st.button('Reset', key='clear')

# st.write("**Added rows:**")
# st.code(st.session_state["my_key"]['added_rows'])

# st.write("**Deleted rows:**")
# st.write(df[['DR_NO','AREA']].iloc[st.session_state["my_key"]['deleted_rows']].to_dict(orient='records'))

# st.write("**Updated rows:**")
# # st.code(list(st.session_state["my_key"]['edited_rows'].keys()))
# st.code(df[['DR_NO','AREA']].iloc[list(st.session_state["my_key"]['edited_rows'].keys())].to_dict(orient='records'))
# st.code(list(st.session_state["my_key"]['edited_rows'].values()))


col1, col2 = st.columns(2)
with col1:
    if st.button("Submit"):
        st.write("**Database State:**")
        for del_row in df[['DR_NO','AREA']].iloc[st.session_state["my_key"]['deleted_rows']].to_dict(orient='records'):
            delete_record(del_row)
            st.code(f"""
                    Record {del_row} deleted successfully
                    """)
            
        for add_row in st.session_state["my_key"]['added_rows']:
            insert_record(add_row)
            st.code(f"""
                    Record {add_row} inserted successfully
                    """)
with col2:
    if st.button("Cancel", type="primary"): 
        st.write('Cancelled. No changes made.')
    