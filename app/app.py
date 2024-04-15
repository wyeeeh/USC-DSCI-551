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


# -- describe data --
for col in df.columns:
    st.write(col, df[col].dtype)

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
@st.cache_data

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



# -- scan & filter data --

## multiselect for categories
def multi_select(df, key):
    default_list = list(set(df[key]))[:3]
    choices = st.multiselect(f"**Choose {str(mapping_keys[key]).lower()}**", set(df[key]), default_list)
    if not choices:
        st.error(f"Please select at least one {str(mapping_keys[key]).lower()}.")
        return df
    else:
        data = df[df[key].isin(choices)]
        return data
    
## slider for numeric values
def slider(df, key):
    default_range = (df[key].min()+100, df[key].max()-100)
    range = st.slider("Select range", df[key].min(), df[key].max(),  default_range)
    if not range:
        st.error("Please select a range.")
        return df
    else:
        data = df[(df[key] >= range[0]) & (df[key] <= range[1])]
        return data
    

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
    now = datetime.datetime.now()
    start_time = st.time_input(label = "Select a time", value = datetime.time(0, 0), step=900)
    end_time = st.time_input(label = "Select a time", value = datetime.time(23, 59), step=900)
    range = [start_time, end_time]
    if not range:
        st.error("Please select a time range.")
        return df
    elif len(range) == 2:
        data = df[(pd.to_datetime(df[key]) >= pd.to_datetime(range[0])) & (pd.to_datetime(df[key]) <= pd.to_datetime(range[1]))]
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

## title & about the data
st.write("""
         
# Crime in Los Angeles [2020 - Present]
**Data Source:** [Los Angeles City Crime Database](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8)
         
""")

## system information
st.caption(
    f":black[Dataset Directory:]  `{os.path.join(data_dir,data)}`"
)

## page links
st.page_link("app.py", label="Home", icon="🏠")
st.page_link("pages/select.py", label="Search", icon="1️⃣")
# st.page_link("pages/page_2.py", label="Page 2", icon="2️⃣", disabled=True)
# st.page_link("http://www.google.com", label="Google", icon="🌎")



## show data
# data = update(df, key = "AREA NAME")
data = date_select(df, key = "DATE OCC")
# data = time_select(df, key = "TIME OCC")
st.write("### Data Table", data)


st.bar_chart(data, x="TIME OCC", y="Crm Cd 1", color="AREA NAME", use_container_width=True)
st.bar_chart(data['AREA NAME'].value_counts(), use_container_width=True)
st.map(data=data, latitude=data['LAT'], longitude=data['LON'], color=None, size=None, zoom=None, use_container_width=True)