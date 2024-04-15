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


## filter functions

### multiselect for categories
def multi_select(df, key):
    default_list = list(set(df[key]))[:3]
    all_list = list(set(df[key]))
    all_list.append("All")
    all_list.sort()
    choices = st.multiselect(f"**Choose {str(mapping_keys[key]).lower()}**", all_list, default_list)
    if not choices:
        st.error(f"Please select at least one {str(mapping_keys[key]).lower()}.")
        return df
    else:
        data = df[df[key].isin(choices)]
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
    default_range = (datetime.date(today.year, 1, 7), today)
    range = st.date_input("Select date range", 
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
        start_time = st.time_input(label = "Time start", value = datetime.time(0, 0), step=900)
    with col2:
        end_time = st.time_input(label = "Time end", value = datetime.time(23, 59), step=900)
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
    if key in ['DATE OCC', 'Date Rptd']:
        df = date_select(df, key)
    elif key in ['AREA NAME']:
        df = multi_select(df, key)
    elif key in ['TIME OCC']:
        df = time_select(df, key)
    return df


# -- create pages --
st.write("""
         
# Data Explorer
**Data Source:** [Los Angeles Domestic Violence Calls Database](https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/)
         
""")

## page links
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.page_link("app.py", label="Home", icon="🏠")
with col2:
    st.page_link("pages/scan & filter.py", label="Scan & Filter", icon="1️⃣")
# st.page_link("pages/page_2.py", label="Page 2", icon="2️⃣", disabled=True)
# st.page_link("http://www.google.com", label="Google", icon="🌎")

## filters
st.write('## Area Selector')
data = multi_select(df, key = "AREA NAME")
st.write('## Date Selector')
data = date_select(data, key = "DATE OCC")
st.write('## Time Selector')
data = time_select(data, key = "TIME OCC")
st.write('## Victim Age Selector')
data = slider(data, key = "Vict Age")

## show data
st.write("## Data Table", data)

st.bar_chart(data = data, x="TIME OCC", y="Crm Cd 1", color="AREA NAME", use_container_width=True)
st.bar_chart(data = data['AREA NAME'].value_counts(), use_container_width=True)
st.map(data=data, latitude=data['LAT'], longitude=data['LON'], color=None, size=None, zoom=None, use_container_width=True)