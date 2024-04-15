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
# @st.cache_data(experimental_allow_widgets=True)
## add data
def add(df, key, value):
    df[key] = value
    return df

## drop data
def drop(df, remove_key="AREA NAME"):
    df = df.drop(remove_key, axis=1)
    return df

## sort data
def sort(df, sort_key="DR_NO", sort_order="asc"):
    df = df.sort_values(by=sort_key, ascending=sort_order)
    return df

## select data
def select(df, select_key):
    return df[select_key]

## filter data
def filter(df, filter_key="AREA NAME", filter_value="Hollywood"):
    df = df[df[filter_key] == filter_value]
    return df

## slice data
def slice(df, start=0, end=10):
    df = df[start:end]
    return df

## map data
def map(df, map_key="AREA NAME"):
    return df[map_key].value_counts()

## group data
def group(df, group_key="AREA NAME"):
    df = df.groupby(group_key).count()
    return df

## summarize data
def summarize(df):
    return df.describe()



# -- create pages --

## title & about the data
st.write("""
         
# Los Angeles Domestic Violence Calls [2020 - Present]
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)
         
""")

## system information
# st.caption(
#     f":black[Dataset Directory:]  `{os.path.join(data_dir,data)}`"
# )

## page links
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.page_link("app.py", label="Home", icon="🏠")
with col2:
    st.page_link("pages/scan & filter.py", label="Scan & Filter", icon="1️⃣")
# st.page_link("pages/page_2.py", label="Page 2", icon="2️⃣", disabled=True)
# st.page_link("http://www.google.com", label="Google", icon="🌎")



# -- describe data --

dict = {}
for col in df.columns:
    dict[col] = [df[col].dtype]
dict.values()

st.dataframe(pd.DataFrame(dict, index=['dtype']))
