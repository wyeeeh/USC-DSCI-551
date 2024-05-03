import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'dsci-551'
}

'''
Inserting Tips:
If key corresponds to a null value in the dictionary inserted, mysql displays a null value. 
If there is no key corresponding to the field, it is NULL in mysql.
A record that does not contain an area field is not allowed to be inserted.
If any of the three fields DR_NO, AREA NAME, Area and the existing data are repeated, an error will be reported
'''


def create_database_connection(area):
    try:
        # First connect to the MySQL server without specifying the database
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor()
        # Create a database name such as Crime_1
        db_name = f'Crime_{int(area)}'  
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        cursor.close()
        
        # Connect to the latest database
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_name,
            auth_plugin='mysql_native_password'
        )
        if conn.is_connected():
            print(f"Connected to database {db_name}")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None
    
   
#insert 
def insert_record(data):
    """Insert records into the appropriate databases and tables"""
    if 'AREA' not in data:
        print("Error: AREA field is missing in the provided data.")
        return
    
    area = data['AREA']
    conn = create_database_connection(area)
    if conn is None:
        return  # If the connection fails, return

    cursor = conn.cursor()
    try:
        # Replace missing values with None
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        # Insert data into the CrimeIncident table
        cursor.execute("""
        INSERT INTO CrimeIncident (DR_NO, Date_Rptd, DATE_OCC, TIME_OCC, AREA, Rpt_Dist_No, Part_1_2, Mocodes, Status, Status_Desc)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Date_Rptd=VALUES(Date_Rptd), DATE_OCC=VALUES(DATE_OCC), TIME_OCC=VALUES(TIME_OCC), Part_1_2=VALUES(Part_1_2), Mocodes=VALUES(Mocodes), Status=VALUES(Status), Status_Desc=VALUES(Status_Desc);
        """, (data.get('DR_NO'), data.get('Date Rptd'), data.get('DATE OCC'), data.get('TIME OCC'), data.get('AREA'), data.get('Rpt Dist No'), data.get('Part 1-2'), data.get('Mocodes'), data.get('Status'), data.get('Status Desc')))

        # Insert data into the Area table
        cursor.execute("""
        INSERT INTO Area (AREA, AREA_NAME)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE AREA=VALUES(AREA);
        """, (data.get('AREA'), data.get('AREA NAME')))

        # Insert data into the Victim table
        cursor.execute("""
        INSERT INTO Victim (DR_NO, Vict_Age, Vict_Sex, Vict_Descent)
        VALUES (%s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('Vict Age'), data.get('Vict Sex'), data.get('Vict Descent')))
            
        # Insert data into the PremiseWeapon table
        cursor.execute("""
        INSERT INTO PremiseWeapon (DR_NO, Premis_Cd, Premis_Desc, Weapon_Used_Cd, Weapon_Desc)
        VALUES (%s, %s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('Premis Cd'), data.get('Premis Desc'), data.get('Weapon Used Cd'), data.get('Weapon Desc')))
       
         
        # Insert data into the IncidentCrimeCode table
        cursor.execute("""
        INSERT INTO IncidentCrimeCode (DR_NO, Crime_Code, Cri_Cd1, Cri_Cd2, Cri_Cd3, Cri_Cd4)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data.get('DR_NO'), data.get('Crm Cd'), data.get('Crm Cd 1'), data.get('Crm Cd 2'), data.get('Crm Cd 3'), data.get('Crm Cd 4')))


        # Insert data into the Location table
        cursor.execute("""
        INSERT INTO Location (DR_NO, Loc, Cro_Street, Lat, Lon)
        VALUES (%s, %s, %s, %s, %s);
        """, (data.get('DR_NO'), data.get('LOCATION'), data.get('Cross Street'), data.get('LAT'), data.get('LON')))

        conn.commit()
        print("Record inserted successfully")
    except Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()



# 假设获取的数据是这样的字典
sample_data = {
    'DR_NO': 'AA123456',
    'Date Rptd': '2021-01-01',
    'DATE OCC': '2021-01-01',
    'TIME OCC': '12:00',
    'AREA': '1',
    'Rpt Dist No': '025',
    'Part 1-2': 1,
    'Mocodes': '1100',
    'Status': 'IC',
    'Status Desc': 'Investigation Continued',
    'Vict Age': '35',
    'Vict Descent': ''
}

insert_record(sample_data)

