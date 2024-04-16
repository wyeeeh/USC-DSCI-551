import streamlit as st
import pandas as pd
import numpy as np
import os
import datetime
import time


# -- data repo --
data_dir = os.path.join(os.getcwd(), "data")
data = "sample_data.csv"

# -- load data --
df = pd.read_csv(os.path.join(data_dir,data)).set_index("DR_NO")
## convert date & time format
df['Date Rptd'] = pd.to_datetime(df['Date Rptd']).dt.strftime('%Y-%m-%d')
df['DATE OCC'] = pd.to_datetime(df['DATE OCC']).dt.strftime('%Y-%m-%d')
df['TIME OCC'] = df['TIME OCC'].astype(str).str.zfill(4)
df['TIME OCC'] = pd.to_datetime(df['TIME OCC'], format='%H%M').dt.strftime('%H:%M')
# -- data repo --
data_dir = os.path.join(os.getcwd(), "data")
data = "sample_data.csv"

# -- load data --
df = pd.read_csv(os.path.join(data_dir,data)).set_index("DR_NO")
## convert date & time format
df['Date Rptd'] = pd.to_datetime(df['Date Rptd']).dt.strftime('%Y-%m-%d')
df['DATE OCC'] = pd.to_datetime(df['DATE OCC']).dt.strftime('%Y-%m-%d')
df['TIME OCC'] = df['TIME OCC'].astype(str).str.zfill(4)
df['TIME OCC'] = pd.to_datetime(df['TIME OCC'], format='%H%M').dt.strftime('%H:%M')


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
def query_data(df, query):
    data = df
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
         
# Data Editor
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present         
""")

## query data

st.write('### 📝 Edit Data')

st.data_editor(df, key="my_key", num_rows="dynamic") # 👈 Set a key
st.write("Here's the value in Session State:")
st.write(st.session_state["my_key"]) # 👈 Show the value in Session State