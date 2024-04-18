import streamlit as st
import pandas as pd
import numpy as np
import os
import datetime
import time


# -- data repo --
data_dir = os.path.join(os.getcwd(), "data")
data = "domestic_violence_calls.csv"

# -- load data --
df = pd.read_csv(os.path.join(data_dir,data))
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


# -- Table Schema --

## CrimeIncident
CrimeIncident_schema = [
    ("DR_NO", "VARCHAR(255)", True),
    ("Date_Rptd", "DATE", False),
    ("DATE_OCC", "DATE", False),
    ("TIME_OCC", "VARCHAR(10)", False),
    ("AREA", "VARCHAR(10)", False),
    ("Rpt_Dist_No", "VARCHAR(10)", False),
    ("Part_1_2", "INT", False),
    ("Mocodes", "TEXT", False),
    ("Status", "VARCHAR(50)", False),
    ("Status_Desc", "VARCHAR(255)", False)
]
CrimeIncident = pd.DataFrame(CrimeIncident_schema, columns=["Schema", "Data Type", "Primary Key"])
CrimeIncident["Primary Key"] = CrimeIncident["Primary Key"].astype(bool)

## Area
Area_schema = [
    ("AREA", "VARCHAR(10)", True),
    ("AREA_NAME", "VARCHAR(255)", False)
]
Area = pd.DataFrame(Area_schema, columns=["Schema", "Data Type", "Primary Key"])
Area["Primary Key"] = Area["Primary Key"].astype(bool)

## Victim
Victim_schema = [
    ("DR_NO", "VARCHAR(255)", True, "CrimeIncident(DR_NO)"),
    ("Vict_Age", "VARCHAR(10)", False, None),
    ("Vict_Sex", "VARCHAR(1)", False, None),
    ("Vict_Descent", "VARCHAR(10)", False, None)
]
Victim = pd.DataFrame(Victim_schema, columns=["Schema", "Data Type", "Primary Key", "Foreign Key"])
Victim["Primary Key"] = Victim["Primary Key"].astype(bool)

## IncidentCrimeCode
IncidentCrimeCode_schema = [
    ("DR_NO", "VARCHAR(255)", True, "CrimeIncident(DR_NO)"),
    ("Crime_Code", "VARCHAR(255)", False, None),
    ("Cri_Cd1", "VARCHAR(255)", False, None),
    ("Cri_Cd2", "VARCHAR(255)", False, None),
    ("Cri_Cd3", "VARCHAR(255)", False, None),
    ("Cri_Cd4", "VARCHAR(255)", False, None)
]
IncidentCrimeCode = pd.DataFrame(IncidentCrimeCode_schema, columns=["Schema", "Data Type", "Primary Key", "Foreign Key"])
IncidentCrimeCode["Primary Key"] = IncidentCrimeCode["Primary Key"].astype(bool)

## PremiseWeapon
PremiseWeapon_schema = [
    ("DR_NO", "VARCHAR(255)", True, "CrimeIncident(DR_NO)"),
    ("Premis_Cd", "VARCHAR(255)", False, None),
    ("Premis_Desc", "VARCHAR(255)", False, None),
    ("Weapon_Used_Cd", "VARCHAR(255)", False, None),
    ("Weapon_Desc", "VARCHAR(255)", False, None)
]
PremiseWeapon = pd.DataFrame(PremiseWeapon_schema, columns=["Schema", "Data Type", "Primary Key", "Foreign Key"])
PremiseWeapon["Primary Key"] = PremiseWeapon["Primary Key"].astype(bool)

## Location
Location_schema = [
    ("DR_NO", "VARCHAR(255)", True, "CrimeIncident(DR_NO)"),
    ("Loc", "VARCHAR(255)", False, None),
    ("Cro_Street", "VARCHAR(255)", False, None),
    ("Lat", "DOUBLE", False, None),
    ("Lon", "DOUBLE", False, None)
]
Location = pd.DataFrame(Location_schema, columns=["Schema", "Data Type", "Primary Key", "Foreign Key"])
Location["Primary Key"] = Location["Primary Key"].astype(bool)



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

## sidebar
with st.sidebar:
    home = st.page_link("app.py", label="Home", icon="🏠")
    scan_filter = st.page_link("pages/scan&filter.py", label="Data Explorer", icon="🔍")
    query = st.page_link("pages/query.py", label="Query Explorer", icon="⌨️")
    edit = st.page_link("pages/edit.py", label="Data Editor", icon="📝")
    

## title & about the data
st.write("""
         
# Los Angeles Domestic Violence Calls

         
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)  
**Data Updated:** 2020 - Present
 
""")


# -- describe data --

dict = {}
subset = df.dropna()
for col in subset.columns:
    if subset is not None and subset[col].iloc[0] is not None:
        dict[col] = [type(subset[col].iloc[0]).__name__, subset[col].iloc[0]]
    else:
        dict[col] = [type(df[col].iloc[0]).__name__, df[col].iloc[0]]

## Table Overview
st.write('### 💾 Data Overview')
st.write(pd.DataFrame(dict, index=['Data Type', 'Sample']), use_container_width=True)

## Table Schema
st.write('### 📑 Database Entity Relationship ')

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["**1️⃣ CrimeIncident**", "**2️⃣ Area**", "**3️⃣ Victim**", "**4️⃣IncidentCrimeCode**", "**5️⃣ PremiseWeapon**", "**6️⃣ Location**"])
with tab1:
    st.write(CrimeIncident, use_container_width=True)
with tab2:
    st.write(Area, use_container_width=True)
with tab3:
    st.write(Victim, use_container_width=True)
with tab4:
    st.write(IncidentCrimeCode, use_container_width=True)
with tab5:
    st.write(PremiseWeapon, use_container_width=True)
with tab6:
    st.write(Location, use_container_width=True)

st.write('### 👩‍💻 Contact Us')
st.write("""
We are a team of masters in data science at University of California. Please contact us if you have any questions.  
- **Ye Wang**: [ywang115@usc.edu](mailto:ywang115@usc.edu)
- **Jie Bian**: [jiebian@usc.edu](mailto:jiebian@usc.edu)
- **Jiani Tang**: [jianitan@usc.edu](mailto:jianitan@usc.edu)
""")