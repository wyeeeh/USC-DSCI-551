import pandas as pd
from datetime import datetime
import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy import text
import mysql.connector
from mysql.connector import Error


# -- Config work directory --
work_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
data_dir = os.path.join(work_dir, "data")


# -- Config MySQL database --
database_dir = os.path.join(work_dir, "database")
key = pd.read_json(os.path.join(database_dir,'key.json'), typ = 'series')
db_config = key.to_dict()

# Print key
for key, value in db_config.items():
    print(f"{key}: {value}")

# -- Read data --
def read_data(file_name):
    df = pd.read_csv(os.path.join(os.path.join(data_dir, file_name)))
    return df

# -- Process data --
def process_data(df):
    print (f"Data loaded with {len(df)} rows.")
    for col in df.columns:
        df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('-','_')
    # Apply the conversion function to the columns
    df['DATE_RPTD'] = pd.to_datetime(df['DATE_RPTD'], format='%m/%d/%Y %H:%M:%S %p').dt.strftime('%Y-%m-%d')
    df['DATE_OCC'] = pd.to_datetime(df['DATE_OCC'], format='%m/%d/%Y %H:%M:%S %p').dt.strftime('%Y-%m-%d')
    df['TIME_OCC'] = df['TIME_OCC'].astype(str).str.zfill(4)
    df['TIME_OCC'] = pd.to_datetime(df['TIME_OCC'], format='%H%M').dt.strftime('%H:%M:%S')

    # Filter out domestic violence calls that are not in the correct area
    df = df[~(df['DR_NO'].astype(str).apply(lambda x: x[2:4]) != df['AREA'].astype(str).apply(lambda x: x.zfill(2)))]
    

    print(f"Data processed with {len(df)} rows.")
    return df

# -- Show data types --
def show_data_types(df):
    subset = df.dropna()
    for col in subset.columns:
        if subset is not None and subset[col].iloc[0] is not None:
            print(col, type(subset[col].iloc[0]).__name__, subset[col].iloc[0])
        else:
            print(type(df[col].iloc[0]).__name__, df[col].iloc[0])

# -- Table Schema --
CrimeIncident_schema =  {
    "DR_NO": "int64",
    "DATE_RPTD": "str",
    "DATE_OCC": "str",
    "TIME_OCC": "str",
    "AREA": "int64",
    "RPT_DIST_NO": "int64",
    "PART_1_2": "int64",
    "MOCODES": "str",
    "PREMIS_CD": "int64",
    "WEAPON_USED_CD": "float64", #int64 null
    "STATUS": "str"
    }

Area_schema =  {
    "AREA": "int64",
    "AREA_NAME": "str"
    }


CrimeCd_schema =  {
    'DR_NO': "int64",
    'CRM_CD': "int64",
    'CRM_CD_1': "int64",
    'CRM_CD_2': "float64", #int64 null
    'CRM_CD_3': "float64", #int64 null
    'CRM_CD_4': "float64", #int64 null
    }

Crime_schema =  {
    'CRM_CD': "int64",
    'CRM_CD_DESC': "str"
    }


Victim_schema =  {
    'DR_NO': "int64",
    'VICT_AGE': "int64",
    'VICT_SEX': "str",
    'VICT_DESCENT': "str"
    }

Premise_schema =  {
    'PREMIS_CD': "int64",
    'PREMIS_DESC': "str"
    }

Weapon_schema =  {
    'WEAPON_USED_CD': "float64",
    'WEAPON_DESC': "str"
    }

Status_schema =  {
    'STATUS': "str",
    'STATUS_DESC': "str"
    }

Location_schema =  {
    'DR_NO': "int64",
    'LOCATION': "str",
    'CROSS_STREET': "str",
    'LAT': "float64",
    'LON': "float64"
    }

# -- Create database --
# Create database
def create_connection(db_config):
    try:
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            auth_plugin='mysql_native_password'
        )
        print("MySQL Database connection successful")
        return conn
    except Error as err:
        print(f"Error: '{err}'")
    


# Reset database
def reset_database(db_config, db_name):
    try:
        conn = create_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        conn.commit()
        cursor.close()
        print(f"Database {db_name} dropped successfully")
    except Error as err:
        print(f"Error: '{err}'")



def create_database(db_config, db_name):
    try:
        conn = create_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        cursor.close()
        print(f"Database {db_name} created successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Create tables using MySQL Connector
def create_tables_mysql_connector(db_config, db_name):
    try:
        conn = create_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")

        if conn.is_connected():
            print(f"Connected to database {db_name}")

            # Area table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Area (
                AREA INT PRIMARY KEY,
                AREA_NAME TEXT
            );
            """)


            # Premise info table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Premise (
                PREMIS_CD INT PRIMARY KEY,
                PREMIS_DESC TEXT
            );
            """)

            # Weapon table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Weapon (
                WEAPON_USED_CD INT PRIMARY KEY,
                WEAPON_DESC TEXT
            );
            """)

            # Status table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Status (
                STATUS VARCHAR(255) PRIMARY KEY,
                STATUS_DESC TEXT
            );
            """)

            # CrimeIncident table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS CrimeIncident (
                DR_NO INT PRIMARY KEY,
                DATE_RPTD DATE,
                DATE_OCC DATE,
                TIME_OCC TIME,
                AREA INT,
                RPT_DIST_NO INT,
                PART_1_2 INT,
                MOCODES TEXT,
                PREMIS_CD INT NULL,
                WEAPON_USED_CD INT NULL,
                STATUS VARCHAR(255) NULL,
                FOREIGN KEY (AREA) REFERENCES Area(AREA),
                FOREIGN KEY (PREMIS_CD) REFERENCES Premise(PREMIS_CD),
                FOREIGN KEY (WEAPON_USED_CD) REFERENCES Weapon(WEAPON_USED_CD),
                FOREIGN KEY (STATUS) REFERENCES Status(STATUS)
            );
            """)

            # Crime table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Crime (
                CRM_CD INT PRIMARY KEY,
                CRM_CD_DESC TEXT
            );
            """)

            # Crime Code table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS CrimeCd (
                DR_NO INT PRIMARY KEY,
                CRM_CD INT,
                CRM_CD_1 INT NULL,
                CRM_CD_2 INT NULL,
                CRM_CD_3 INT NULL,
                CRM_CD_4 INT NULL,
                FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO),
                FOREIGN KEY (CRM_CD) REFERENCES Crime(CRM_CD)
            );
            """)

            # Victim table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Victim (
                DR_NO INT PRIMARY KEY,
                VICT_AGE INT,
                VICT_SEX TEXT,
                VICT_DESCENT TEXT,
                FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
            );
            """)

            # Location table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Location (
                DR_NO INT PRIMARY KEY,
                LOCATION TEXT,
                CROSS_STREET TEXT,
                LAT FLOAT,
                LON FLOAT,
                FOREIGN KEY (DR_NO) REFERENCES CRIMEINCIDENT(DR_NO)
            );
            """)

            conn.commit()
            cursor.close()
            print("Tables created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        return None
    
# Upload data using SQLAlchemy
def uplaod_data(data, db_config, db_name, rows=None):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    print (f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}/{db_name}')
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}/{db_name}', echo=False)
        with engine.connect() as conn:
            data[Area_schema.keys()].drop_duplicates().dropna().to_sql(name = 'Area', con = engine, if_exists = 'append', index = False)
            data[Premise_schema.keys()].drop_duplicates().to_sql(name = 'Premise', con = engine, if_exists = 'append', index = False)
            data[Weapon_schema.keys()].drop_duplicates().dropna().to_sql(name = 'Weapon', con = engine, if_exists = 'append', index = False)
            data[Status_schema.keys()].drop_duplicates().dropna().to_sql(name = 'Status', con = engine, if_exists = 'append', index = False)
            data[Crime_schema.keys()].drop_duplicates().dropna().to_sql(name = 'Crime', con = engine, if_exists = 'append', index = False)
            if rows:
                data[CrimeIncident_schema.keys()].head(rows).to_sql(name = 'CrimeIncident', con = engine, if_exists = 'append', index = False)
                data[CrimeCd_schema.keys()].head(rows).to_sql(name = 'CrimeCd', con = engine, if_exists = 'append', index = False)
                data[Victim_schema.keys()].head(rows).to_sql(name = 'Victim', con = engine, if_exists = 'append', index = False)
                data[Location_schema.keys()].head(rows).to_sql(name = 'Location', con = engine, if_exists = 'append', index = False)
            else:
                data[CrimeIncident_schema.keys()].to_sql(name = 'CrimeIncident', con = engine, if_exists = 'append', index = False)
                data[CrimeCd_schema.keys()].to_sql(name = 'CrimeCd', con = engine, if_exists = 'append', index = False)
                data[Victim_schema.keys()].to_sql(name = 'Victim', con = engine, if_exists = 'append', index = False)
                data[Location_schema.keys()].to_sql(name = 'Location', con = engine, if_exists = 'append', index = False)
            conn.commit()
            print("Data uploaded successfully")
    
    except Error as err:
        print(f"Error: '{err}'")
        return None

def query_data(query, db_config, db_name=None):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}', echo=False)
        with engine.connect() as conn:
            if not db_name:
                results = conn.execute(text(query))
                conn.commit()
            else:
                conn.execute(text(f"""USE {db_name};"""))
                # print(f"Connected to database {db_name}")
                conn.commit()
                results = conn.execute(text(query))
            return results.all()
    except Error as err:
        print(f"Error: '{err}'")
        return None



data = process_data(read_data(file_name = 'domestic_violence_calls.csv'))
# reset_database(db_config, 'test_db')

print(query_data(f"""SHOW DATABASES;""", db_config))

# print(query_data('SELECT * FROM CrimeIncident LIMIT 1', db_config, db_name = 'test_db'))

# create database
db_name_lst = data['DR_NO'].astype(str).apply(lambda x: x[2:4]).unique()
db_name_lst.sort()
# print(db_name_lst)

# sum = 0
for db_name in db_name_lst:
    print(db_name)
    print(f"Crime_{db_name}", len(data[data['DR_NO'].astype(str).apply(lambda x: x[2:4]) == db_name]))
    sum += len(data[data['DR_NO'].astype(str).apply(lambda x: x[2:4]) == db_name])
    print(sum)

for db_name in db_name_lst:
    db_name = 'Crime_' + str(db_name)
    # create_database(db_config, db_name)
    # create_tables_mysql_connector(db_config, db_name)
    print(data[data['DR_NO'].astype(str).apply(lambda x: x[2:4]) == db_name].head(100))
    uplaod_data(data[data['DR_NO'].astype(str).apply(lambda x: x[2:4]) == db_name], db_config, db_name)
    # print(query_data(f"""SHOW DATABASES;""", db_config))
    print(query_data(f"""SHOW TABLES;""", db_config, db_name = db_name))
    print(query_data(f"""SELECT * FROM CrimeIncident LIMIT 10;""", db_config, db_name = db_name))


