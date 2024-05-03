import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import os
from preprocessData import process_data


work_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# Read the key file and config MySQL database
database_dir = os.path.join(work_dir, "database")
key = pd.read_json(os.path.join(database_dir,'key.json'), typ = 'series')
db_config = key.to_dict()

# Print key
for key, value in db_config.items():
    print(f"{key}: {value}")

def read_data(file_name):
    
    # Read data
    work_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    data_dir = os.path.join(work_dir, "data")
    data = pd.read_csv(os.path.join(data_dir,file_name))
    return data

CrimeIncident_schema =  {
                'DR_NO': ['int64'],
                'Date Rptd': ['str'],
                'DATE OCC': ['str'],
                'TIME OCC': ['int64'],
                'AREA': ['int64'],
                'Rpt Dist No': ['int64'],
                'Part 1-2': ['int64'],
                'Crm Cd': ['int64'],
                'Mocodes': ['str'],
                'Premis Cd': ['int64'],
                'Weapon Used Cd': ['int64'],
                'Status': ['str']}


Area_schema =  {
 'AREA': ['int64'],
 'AREA NAME': ['str']}

Crime_schema =  {
 'DR_NO': ['int64'],
 'Crm Cd': ['int64'],
 'Crm Cd 1': ['int64'],
 'Crm Cd 2': ['int64'],
 'Crm Cd 3': ['int64'],
 'Crm Cd 4': ['int64']}

Crime_info =  {
 'Crm Cd': ['int64'],
 'Crm Cd Desc': ['str']
 }


Victim_schema =  {
 'DR_NO': ['int64'],
 'Vict Age': ['int64'],
 'Vict Sex': ['str'],
 'Vict Descent': ['str']
 }

Premise_info =  {
 'Premis Cd': ['int64'],
 'Premis Desc': ['str']
 }

Weapon_info =  {
 'Weapon Used Cd': ['int64'],
 'Weapon Desc': ['str']
 }

Status_info =  {
 'Status': ['str'],
 'Status Desc': ['str']
 }

Location_schema =  {
 'DR_NO': ['int64'],
 'LOCATION': ['str'],
 'Cross Street': ['str'],
 'LAT': ['float64'],
 'LON': ['float64']
 }

# Create connection to the MySQL server
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

def create_tables(db_config, db_name):
    try:
        conn = create_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")

        if conn.is_connected():
            print(f"Connected to database {db_name}")

            # CrimeIncident table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS CrimeIncident (
                DR_NO INT PRIMARY KEY,
                Date_Rptd DATE,
                DATE_OCC DATE,
                TIME_OCC TIME,
                AREA INT NULL,
                Rpt_Dist_No INT,
                Part_1_2 INT,
                Mocodes VARCHAR(255),
                Premis_Cd INT,
                Weapon_Used_Cd INT,
                Status VARCHAR(255)
            );
            """)

            # Area table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Area (
                AREA PRIMARY KEY,
                AREA_NAME VARCHAR(255)
            );
            """)

            # Crime table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Crime (
                DR_NO INT PRIMARY KEY,
                Crm_Cd INT,
                Crm_Cd1 INT NULL,
                Crm_Cd2 INT NULL,
                Crm_Cd3 INT NULL,
                Crm_Cd4 INT NULL,
                FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
            );
            """)

            # Crime Info table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS CrimeInfo (
                Crm_Cd INT PRIMARY KEY,
                Crm_Cd_Desc VARCHAR(255),
                FOREIGN KEY (Crm_Cd) REFERENCES Crime(Crm_Cd)
            );
            """)

            # Victim table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Victim (
                DR_NO INT PRIMARY KEY,
                Vict_Age INT,
                Vict_Sex VARCHAR(255),
                Vict_Descent VARCHAR(255),
                FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
            );
            """)

            # Premise table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Premise (
                Premis_Cd INT PRIMARY KEY,
                Premis_Desc VARCHAR(255)
            );
            """)

            # Weapon table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Weapon (
                Weapon_Used_Cd INT PRIMARY KEY,
                Weapon_Desc VARCHAR(255)
            );
            """)

            # Status table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Status (
                Status VARCHAR(255) PRIMARY KEY,
                Status_Desc VARCHAR(255)
            );
            """)

            # Location table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Location (
                DR_NO INT PRIMARY KEY,
                Loc VARCHAR(255),
                Cro_Street VARCHAR(255),
                Lat DOUBLE,
                Lon DOUBLE,
                FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
            );
            """)

            conn.commit()
            cursor.close()
            print("Tables created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        return None



if __name__ == "__main__":
    data = process_data(read_data('domestic_violence_calls.csv'))
    for DR_NO in data.head(1)['DR_NO'].astype(str):
        db_name = 'Crime_' + str(DR_NO[2:4])
        print(db_name)
        create_database(db_config, db_name)
        create_tables(db_config, db_name)
    # insert CrimeIncident data
    conn = create_connection(db_config)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")
        data
        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully")
        



# count = 0 #calculate the number of the total records
# for area in data['AREA'].unique(): #choose AREA as the partition key
#     df_area = data[data['AREA'] == area]
#     conn = create_database_connection(area)
#     if conn:
#         create_tables(conn)
#         count = insert_data(conn, df_area, count)
#         conn.close()
#         print(f"Processing complete for area: {area}")
        
# print(count)
