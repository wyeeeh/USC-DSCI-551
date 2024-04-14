import streamlit as st
import time
import pandas as pd
import numpy as np
import os



# -- data repo --
data_dir = os.path.join(os.getcwd(), "data")
data = "sample_data.csv"

# -- config functions --
@st.cache_data

## add data
def add(df, add_key="AREA NAME", add_value="Hollywood"):
    df[add_key] = add_value
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


# -- load data --
df = pd.read_csv(os.path.join(data_dir,data))
df = df.set_index("DR_NO")


## multiselect areas
def multiselect(df):
    ### areas
    default_areas = ["Hollywood", "Central", "Van Nuys", "Olympic", "Southeast"] 
    areas = st.multiselect(  #areas会返回一个list，里面包含了用户选择的区域 ["Hollywood","Central","Van Nuys", "Olympic", "Southeast"]
    "Choose areas", set(df['AREA NAME']), default_areas)
    if not areas:
        st.error("Please select at least one area.")
        return df
    else:
        data = df[df['AREA NAME'].isin(areas)]
        return data

def update(df):
    return multiselect(df)

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
st.page_link("demo.py", label="Home", icon="🏠")
st.page_link("pages/select.py", label="Search", icon="1️⃣")
# st.page_link("pages/page_2.py", label="Page 2", icon="2️⃣", disabled=True)
# st.page_link("http://www.google.com", label="Google", icon="🌎")

## data table
st.write("### Data Table")
st.write(df, use_container_width=True)


## create charts
data = update(df)

st.write("### Title", data.sort_index())
st.bar_chart(data, x="TIME OCC", y="Crm Cd 1", color="AREA NAME", use_container_width=True)
st.bar_chart(data['AREA NAME'].value_counts(), use_container_width=True)
st.map(data=data, latitude=data['LAT'], longitude=data['LON'], color=None, size=None, zoom=None, use_container_width=True)