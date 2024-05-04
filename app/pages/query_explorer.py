import streamlit as st
import pandas as pd
import numpy as np
import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy import text
import mysql.connector
from mysql.connector import Error


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

# -- config functions --
# @st.cache_data

# -- Config work directory --
work_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
data_dir = os.path.join(work_dir, "data")

# -- Config MySQL database --
database_dir = os.path.join(work_dir, "database")
key = pd.read_json(os.path.join(database_dir,'key.json'), typ = 'series')
db_config = key.to_dict()

# # check key
# for key, value in db_config.items():
#     st.write(f"{key}: {value}")

# -- Query data --
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


# -- create pages --

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    data_explorer = st.page_link("pages/data_explorer.py", label="Data Explorer", icon="🔍")
    query_explorer = st.page_link("pages/query_explorer.py", label="Query Explorer", icon="⌨️")
    editor = st.page_link("pages/editor.py", label="Data Editor", icon="📝")
    

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
    st.write(query(user_query, db_config=db_config))
with tab2:
    st.write('### Query Executed')
    st.code(f"""
    {user_query}
    """)