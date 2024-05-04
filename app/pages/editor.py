import streamlit as st
import pandas as pd
import numpy as np
import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy import text
import mysql.connector
from mysql.connector import Error
import datetime
import time


# -- global variables --
mapping_keys = {
    "DR_NO": "Records Number",
    "DATE_RPTD": "Report Date",
    "DATE_OCC": "Occur Date",
    "TIME_OCC": "Occur Time",
    "AREA": "Area Code",
    "AREA_NAME": "Area",
    "RPT_DIST_NO": "Report District Number",
    "Part_1_2": "Part 1-2",
    "CRM_CD": "Crime Code",
    "CRM_CD_DESC": "Crime",
    "MOCODES": "Mocodes",
    "VICT_AGE": "Victim Age",
    "VICT_SEX": "Victim Sex",
    "VICT_DESCENT": "Victim Descent",
    "PREMIS_CD": "Premise Code",
    "PREMIS_DESC": "Premise",
    "WEAPON_USED_CD": "Weapon Used Code",
    "WEAPON_DESC": "Weapon",
    "STATUS": "Status Code",
    "STATUS_DESC": "Status",
    "CRM_CD_1": "Crime Code 1",
    "CRM_CD_2": "Crime Code 2",
    "CRM_CD_3": "Crime Code 3",
    "CRM_CD_4": "Crime Code 4",
    "LOCATION": "Location",
    "CROSS_STREET": "Cross Street",
    "LAT": "Latitude",
    "LON": "Longitude"
}

table_schema_list = {
    "Area": {"AREA": "INT", "AREA_NAME": "TEXT"},
    "Premise": {"PREMIS_CD": "INT", "PREMIS_DESC": "TEXT"},
    "Weapon": {"WEAPON_USED_CD": "INT", "WEAPON_DESC": "TEXT"},
    "Status": {"STATUS": "VARCHAR(255)", "STATUS_DESC": "TEXT"},
    "CrimeIncident": {"DR_NO": "INT", "DATE_RPTD": "DATE", "DATE_OCC": "DATE", "TIME_OCC": "TIME", "AREA": "INT", "RPT_DIST": "INT", "PART_1_2": "INT", "MOCODES": "TEXT","PREMIS_CD":"INT", "WEAPON_USED_CD":"INT", "STATUS":"VARCHAR(255)"},
    "Crime": {"CRM_CD": "INT", "CRM_CD_DESC": "TEXT"},
    "CrimeCd": {"DR_NO": "INT", "CRM_CD": "INT", "CRM_CD_1": "INT", "CRM_CD_2": "INT", "CRM_CD_3": "INT", "CRM_CD_4": "INT"},
    "Victim": {"DR_NO": "INT", "VICT_AGE": "INT", "VICT_SEX": "TEXT", "VICT_DESCENT": "TEXT"},
    "Location": {"DR_NO": "INT", "LOCATION": "TEXT", "CROSS_STREET": "TEXT", "LAT": "FLOAT", "LON": "FLOAT"}
}

for i in table_schema_list.values():
    print(list(i.keys()))
# -- config functions --
# @st.cache_data

# -- Config work directory --
work_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
data_dir = os.path.join(work_dir, "data")

# -- Config MySQL database --
database_dir = os.path.join(work_dir, "database")
key = pd.read_json(os.path.join(database_dir,'key.json'), typ = 'series')
db_config = key.to_dict()

# -- config functions --
# @st.cache_data

## query functions
host=db_config['host']
user=db_config['user']
password=db_config['password']

# st.write(db_config)
# st.write(host)
# st.write(user)
# st.write(password)

def create_database_connection(area):
    db_name = "Crime_"+str(area)
    try:
        # Connect to the latest database
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_name,
            auth_plugin='mysql_native_password'
        )
        # Create a database name such as Crime_1
        if conn.is_connected():
            print(f"Connected to database {db_name}")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None

def execute_query(query, db_config, db_name=None):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}', echo=False)
        with engine.connect() as conn:
            if not db_name:
                results = conn.execute(text(query))
                conn.commit()
            else:
                # print(f"Connected to database {db_name} successfully")
                conn.execute(text(f"""USE {db_name};"""))
                # print(f"Connected to database {db_name}")
                conn.commit()
                results = conn.execute(text(query))
            return results.all()
    except Error as err:
        print(f"Error: '{err}'")
        return None
    
def select_query(query, db_config, db_name):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}/{db_name}', echo=False)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
    except Error as err:
        print(f"Error: '{err}'")
        return None
    
def query(query, db_config):
    if query.lower().strip().startswith('select'):
        result_df = pd.DataFrame()
        if 'LIMIT' in query.upper():
            all_query = query.upper().split('LIMIT')[0]+';'
            limit = int(query.upper().split('LIMIT')[1].split(';')[0])
            for area_id in range(1,22):
                db_name = "Crime_" + str(area_id).zfill(2)
                result_df = pd.concat([result_df, select_query(all_query, db_config, db_name)])
            result_df = result_df.head(limit)
        else:
            for area_id in range(1,22):
                db_name = "Crime_" + str(area_id).zfill(2)
                result_df = pd.concat([result_df, select_query(query, db_config, db_name)])
        
        if 'TIME_OCC' in result_df.columns:
            result_df['TIME_OCC'] = result_df['TIME_OCC'].astype(str).apply(lambda x: x[-8:])
            result_df['TIME_OCC'] = pd.to_datetime(result_df['TIME_OCC'], format='%H:%M:%S').dt.strftime('%H:%M')
        return result_df.reset_index(drop=True)
    else:
        result = execute_query(query, db_config)
        return result
    
## delete functions

def delete_record(data):
    """Deletes records from the corresponding databases and tables, ensuring no foreign key constraint fails."""
    area = str(data['DR_NO'])[2:4]
    try:
        conn = create_database_connection(area)
        cursor = conn.cursor()
        # Ensure data dictionary values are None if they are NaN (pandas handling)
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        # Delete a record from the Victim
        cursor.execute("""
        DELETE FROM Victim WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Victim")

        # Delete a record from the CrimeCd table
        cursor.execute("""
        DELETE FROM CrimeCd WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from CrimeCd")


        # Delete a record from the Location
        cursor.execute("""
        DELETE FROM Location WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Location")

        # Delete a record from CrimeIncident table
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
    area = str(data['DR_NO'])[2:4]
    try:
        conn = create_database_connection(area)
        cursor = conn.cursor()
        # Replace missing values with None
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        for column in data.keys():
            if column in data.keys():
                # Check if the column exists in the data dictionary
                # Insert data into the corresponding table
                if column == 'AREA':
                    # Insert data into the Area table
                    cursor.execute("""
                    INSERT INTO Area (AREA, AREA_NAME)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE AREA_NAME=VALUES(AREA_NAME);
                    """, (data.get('AREA') if data.get('AREA') else None, data.get('AREA_NAME') if data.get('AREA_NAME') else None))
                elif column == 'PREMIS_CD':
                    # Insert data into the Premise table
                    cursor.execute("""
                    INSERT INTO Premise (PREMIS_CD, PREMIS_DESC)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE PREMIS_DESC=VALUES(PREMIS_DESC);
                    """, (data.get('PREMIS_CD') if data.get('PREMIS_CD') else None, data.get('PREMIS_DESC') if data.get('PREMIS_DESC') else None))
                elif column == 'WEAPON_USED_CD':
                    # Insert data into the Weapon table
                    cursor.execute("""
                    INSERT INTO Weapon (WEAPON_USED_CD, WEAPON_DESC)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE WEAPON_DESC=VALUES(WEAPON_DESC);
                    """, (data.get('WEAPON_USED_CD') if data.get('WEAPON_USED_CD') else None, data.get('WEAPON_DESC') if data.get('WEAPON_DESC') else None))
                elif column == 'STATUS':
                    # Insert data into the Status table
                    cursor.execute("""
                    INSERT INTO Status (STATUS, STATUS_DESC)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE STATUS_DESC=VALUES(STATUS_DESC);
                    """, (data.get('STATUS') if data.get('STATUS') else None, data.get('STATUS_DESC') if data.get('STATUS_DESC') else None))
                elif column == 'CRM_CD':
                    # Insert data into the Crime table
                    cursor.execute("""
                    INSERT INTO Crime (CRM_CD, CRM_CD_DESC)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE CRM_CD_DESC=VALUES(CRM_CD_DESC);
                    """, (data.get('CRM_CD') if data.get('CRM_CD') else None, data.get('CRM_CD_DESC') if data.get('CRM_CD_DESC') else None))
                elif column == 'DR_NO':
                    # Insert data into the CrimeIncident table
                    cursor.execute("""
                    INSERT INTO CrimeIncident (DR_NO, DATE_RPTD, DATE_OCC, TIME_OCC, AREA, RPT_DIST, PART_1_2, MOCODES, PREMIS_CD, WEAPON_USED_CD, STATUS)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE DATE_RPTD=VALUES(DATE_RPTD), DATE_OCC=VALUES(DATE_OCC), TIME_OCC=VALUES(TIME_OCC), AREA=VALUES(AREA), RPT_DIST=VALUES(RPT_DIST), PART_1_2=VALUES(PART_1_2), MOCODES=VALUES(MOCODES), PREMIS_CD=VALUES(PREMIS_CD), WEAPON_USED_CD=VALUES(WEAPON_USED_CD), STATUS=VALUES(STATUS);
                    """, (data.get('DR_NO') if data.get('DR_NO') else None, data.get('DATE_RPTD') if data.get('DATE_RPTD') else None, data.get('DATE_OCC') if data.get('DATE_OCC') else None, data.get('TIME_OCC') if data.get('TIME_OCC') else None, data.get('AREA') if data.get('AREA') else None, data.get('RPT_DIST') if data.get('RPT_DIST') else None, data.get('PART_1_2') if data.get('PART_1_2') else None, data.get('MOCODES') if data.get('MOCODES') else None, data.get('PREMIS_CD') if data.get('PREMIS_CD') else None, data.get('WEAPON_USED_CD') if data.get('WEAPON_USED_CD') else None, data.get('STATUS') if data.get('STATUS') else None))
                elif column == 'CRM_CD_1':
                    # Insert data into the CrimeCd table
                    cursor.execute("""
                    INSERT INTO CrimeCd (DR_NO, CRM_CD, CRM_CD_1, CRM_CD_2, CRM_CD_3, CRM_CD_4)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE CRM_CD_1=VALUES(CRM_CD_1), CRM_CD_2=VALUES(CRM_CD_2), CRM_CD_3=VALUES(CRM_CD_3), CRM_CD_4=VALUES(CRM_CD_4);
                    """, (data.get('DR_NO') if data.get('DR_NO') else None, data.get('CRM_CD') if data.get('CRM_CD') else None, data.get('CRM_CD_1') if data.get('CRM_CD_1') else None, data.get('CRM_CD_2') if data.get('CRM_CD_2') else None, data.get('CRM_CD_3') if data.get('CRM_CD_3') else None, data.get('CRM_CD_4') if data.get('CRM_CD_4') else None))
                elif column == 'VICT_AGE':
                    # Insert data into the Victim table
                    cursor.execute("""
                    INSERT INTO Victim (DR_NO, VICT_AGE, VICT_SEX, VICT_DESCENT)
                    VALUES (%s, %s, %s, %s);
                    """, (data.get('DR_NO') if data.get('DR_NO') else None, data.get('VICT_AGE') if data.get('VICT_AGE') else None, data.get('VICT_SEX') if data.get('VICT_SEX') else None, data.get('VICT_DESCENT') if data.get('VICT_DESCENT') else None))
                elif column == 'LOCATION':
                    # Insert data into the Location table
                    cursor.execute("""
                    INSERT INTO Location (DR_NO, LOCATION, CROSS_STREET, LAT, LON)
                    VALUES (%s, %s, %s, %s, %s);
                    """, (data.get('DR_NO') if data.get('DR_NO') else None, data.get('LOCATION') if data.get('LOCATION') else None, data.get('CROSS_STREET') if data.get('CROSS_STREET') else None, data.get('LAT') if data.get('LAT') else None, data.get('LON') if data.get('LON') else None))
        conn.commit()
        print("Record inserted successfully")
    except Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()




# -- create pages --

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    data_explorer = st.page_link("pages/data_explorer.py", label="Data Explorer", icon="🔍")
    query_explorer = st.page_link("pages/query_explorer.py", label="Query Explorer", icon="⌨️")
    editor = st.page_link("pages/editor.py", label="Data Editor", icon="📝")
    

## title & about the data
st.write("""
# Data Editor
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present         
""")


# -- load data
select_all_query = """
                       SELECT ci.*, a.AREA_NAME, p.PREMIS_DESC, w.WEAPON_DESC, s.STATUS_DESC, cc.CRM_CD, cc.CRM_CD_DESC, cc.CRM_CD_1, cc.CRM_CD_2, cc.CRM_CD_3, cc.CRM_CD_4, v.VICT_AGE, v.VICT_SEX, v.VICT_DESCENT, l.LOCATION, l.CROSS_STREET, l.LAT, l.LON
                        FROM CrimeIncident ci
                        LEFT JOIN Area a ON ci.AREA = a.AREA
                        LEFT JOIN Premise p ON ci.PREMIS_CD = p.PREMIS_CD
                        LEFT JOIN Weapon w ON ci.WEAPON_USED_CD = w.WEAPON_USED_CD
                        LEFT JOIN Status s ON ci.STATUS = s.STATUS
                        LEFT JOIN (SELECT cc.*, cm.CRM_CD_DESC FROM CrimeCd cc LEFT JOIN Crime cm ON cc.CRM_CD = cm.CRM_CD) cc ON ci.DR_NO = cc.DR_NO
                        LEFT JOIN Victim v ON ci.DR_NO = v.DR_NO
                        LEFT JOIN Location l ON ci.DR_NO = l.DR_NO;
                        """
df = query(select_all_query, db_config)

st.write('### 📝 Edit Data')

# st.data_editor(df, key="my_key", disabled=("DR_NO", "AREA"), num_rows="dynamic") # 👈 Set a key
st.data_editor(df, key="my_key", num_rows="dynamic") # 👈 Set a key
# st.write("Here's the value in Session State:")

st.write("**Change Preview:**")
st.write(st.session_state["my_key"]) # 👈 Show the value in Session State

# if st.session_state.get('clear'):
#     st.session_state['my_key'] = ''

# st.button('Reset', key='clear')

st.write("**Added rows:**")
added_rows = st.session_state["my_key"]['added_rows']  # added_rows 
st.code(added_rows)

st.write("**Deleted rows:**")
del_rows = df[['DR_NO']].iloc[st.session_state["my_key"]['deleted_rows']].to_dict(orient='records')
st.write(del_rows)

# st.write("**Updated rows:**")
# # st.code(list(st.session_state["my_key"]['edited_rows'].keys()))
# st.code(df[['DR_NO']].iloc[list(st.session_state["my_key"]['edited_rows'].keys())].to_dict(orient='records'))
# st.code(list(st.session_state["my_key"]['edited_rows'].values()))


col1, col2 = st.columns(2)
with col1:
    if st.button("Submit"):
        st.write("**Database State:**")
        for del_row in del_rows:
            # st.code(f"""{del_row}""")
            delete_record(del_row)
            st.code(f"""
                    Record {del_row} deleted successfully
                    """)
            
        for add_row in st.session_state["my_key"]['added_rows']:
            st.code(f"""{add_row}""")
            insert_record(add_row)
            st.code(f"""
                    Record {add_row} inserted successfully
                    """)
with col2:
    if st.button("Cancel", type="primary"): 
        st.write('Cancelled. No changes made.')
    