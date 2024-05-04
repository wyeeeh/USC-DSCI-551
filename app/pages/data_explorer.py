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

# -- config functions --
# @st.cache_data

## filter functions

### multiselect for categories
def multi_select(df, key):
    default_list = list(set(df[key]))
    default_list.sort()
    default_list = default_list[:2]
    all_list = list(set(df[key]))
    all_list.sort()
    all_list.insert(0, "All Areas")
    choices = st.multiselect(f"**Choose {str(mapping_keys[key]).lower()}**", all_list, default_list)
    if not choices:
        st.error(f"Please select at least one {str(mapping_keys[key]).lower()}.")
        return df
    elif "All Areas" in choices:
        return df
    else:
        data = df[df[key].isin(choices)] #change into isin
        return data
    
### slider for numeric values
def slider(df, key):
    default_range = (round(df[key].max()/5) if df[key].max() > 0 else 0, round(df[key].max()*4/5) if df[key].max() > 0 else 80)
    range = st.slider(f"**Select range of {str(mapping_keys[key]).lower()}**", df[key].min() if df[key].min() > 0 else 0, df[key].max() if df[key].max() > 0 else 100,  default_range)
    if not range:
        st.error(f"Please select a range of {str(mapping_keys[key]).lower()}.")
        return df
    else:
        data = df[(df[key] >= range[0]) & (df[key] <= range[1])]
        return data
    
### select for date & time
def date_select(df, key):
    today = datetime.datetime.now()
    default_range = (datetime.date(today.year-1, 1, 1), today)
    range = st.date_input("**Select date range**", 
                          default_range,
                          datetime.date(2020, 1, 1),
                          datetime.date(today.year, 12, 31),
                          format="MM/DD/YYYY"
                          )
    if not range:
        st.error("Please select a date range.")
        return df
    elif len(range) == 2:
        data = df[(pd.to_datetime(df[key]) >= pd.to_datetime(range[0])) & (pd.to_datetime(df[key]) <= pd.to_datetime(range[1]))]
        return data
    else:
        st.error("Please complete the date range.")
        return df

def time_select(df, key):
    # now = datetime.datetime.now()
    col1, col2 = st.columns(2)
    with col1:
        start_time = st.time_input(label = "**Time start**", value = datetime.time(0, 0), step=900)
    with col2:
        end_time = st.time_input(label = "**Time end**", value = datetime.time(23, 59), step=900)
    range = [start_time, end_time]
    if not range:
        st.error("Please select a time range.")
        return df
    elif range[0] > range[1]:
        st.error("Please select a valid time range.")
        return df
    elif len(range) == 2:
        data =  df[(pd.to_datetime(df[key], format='%H:%M').dt.strftime('%H:%M ') >= range[0].strftime('%H:%M')) & (pd.to_datetime(df[key], format='%H:%M').dt.strftime('%H:%M') <= range[1].strftime('%H:%M'))]
        return data
    else:
        st.error("Please complete the time range.")
        return df


def update(df, key):
    if key in ['DATE_OCC', 'DATE_RPTD']:
        df = date_select(df, key)
    elif key in ['AREA_NAME']:
        df = multi_select(df, key)
    elif key in ['TIME_OCC']:
        df = time_select(df, key)
    return df

## Query functions
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

# -- create pages --

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    data_explorer = st.page_link("pages/data_explorer.py", label="Data Explorer", icon="🔍")
    query_explorer = st.page_link("pages/query_explorer.py", label="Query Explorer", icon="⌨️")
    editor = st.page_link("pages/editor.py", label="Data Editor", icon="📝")
    

## title & about the data
st.write("""
         
# Data Explorer
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present         
""")
st.write(df)


## filters
st.write('### 🗺️ Area Selector')
data = multi_select(df, key = "AREA_NAME")
st.write('### 📆 Date Selector')
data = date_select(data, key = "DATE_OCC")
st.write('### 🕘 Time Selector')
data = time_select(data, key = "TIME_OCC")
st.write('### 🎚️ Victim Age Selector')
data = slider(data, key = "VICT_AGE")

## show data

tab1, tab2, tab3 = st.tabs(["🗺️ Map","📈 Chart", "🗃 Data"])
with tab1:
    st.write("### Crime Distribution")
    st.map(data=data, latitude=data['LAT'], longitude=data['LON'], color=None, size=None, zoom=None, use_container_width=True)
with tab2:
    st.write('### Bar Chart')
    st.bar_chart(data = data, x="TIME_OCC", y="CRM_CD", color="AREA_NAME", use_container_width=True)
with tab3:
    st.write('### Data Result')
    st.write(data[['TIME_OCC', 'CRM_CD', 'AREA_NAME']], use_container_width=True)