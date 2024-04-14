import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'dsci-551'
}


# Read the CSV file
file_path = 'crimedata_processed.csv'
data = pd.read_csv(file_path)

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
    
# Create tables
def create_tables(conn):
    cursor = conn.cursor()
    try:
        # CrimeIncident table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS CrimeIncident (
            DR_NO VARCHAR(255) PRIMARY KEY,
            Date_Rptd DATE,
            DATE_OCC DATE,
            TIME_OCC VARCHAR(10),
            AREA VARCHAR(10),
            Rpt_Dist_No VARCHAR(10),
            Part_1_2 INT,
            Mocodes TEXT,
            Status VARCHAR(50),
            Status_Desc VARCHAR(255)
        );
        """)
        # Area table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Area (
            AREA VARCHAR(10) PRIMARY KEY,
            AREA_NAME VARCHAR(255)
        );
        """)
        # Victim table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Victim (
            DR_NO VARCHAR(255) PRIMARY KEY,
            Vict_Age VARCHAR(10),
            Vict_Sex VARCHAR(1),
            Vict_Descent VARCHAR(10),
            FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
        );
        """)
        # IncidentCrimeCode table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS IncidentCrimeCode (
            DR_NO VARCHAR(255),
            Crime_Code VARCHAR(255),
            Cri_Cd1 VARCHAR(255),
            Cri_Cd2 VARCHAR(255),
            Cri_Cd3 VARCHAR(255),
            Cri_Cd4 VARCHAR(255),
            PRIMARY KEY (DR_NO),
            FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
        );
        """)
        
        # PremiseWeapon table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PremiseWeapon (
            DR_NO VARCHAR(255),
            Premis_Cd VARCHAR(255),
            Premis_Desc VARCHAR(255),
            Weapon_Used_Cd VARCHAR(255),
            Weapon_Desc VARCHAR(255),
            PRIMARY KEY (DR_NO),
            FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
        );
        """)
        
        # Location table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Location (
            DR_NO VARCHAR(255),
            Loc VARCHAR(255),
            Cro_Street VARCHAR(255),
            Lat DOUBLE,
            Lon DOUBLE,
            PRIMARY KEY (DR_NO),
            FOREIGN KEY (DR_NO) REFERENCES CrimeIncident(DR_NO)
        );
        """)

        conn.commit()
        print("Tables created successfully")
    except Error as e:
        print(f"Error creating tables: {e}")

def insert_data(conn, df, count):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        row = row.where(pd.notna(row), None)

        try:
            # Insert data into the CrimeIncident table
            cursor.execute("""
            INSERT INTO CrimeIncident (DR_NO, Date_Rptd, DATE_OCC, TIME_OCC, AREA, Rpt_Dist_No, Part_1_2, Mocodes, Status, Status_Desc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Date_Rptd=VALUES(Date_Rptd), DATE_OCC=VALUES(DATE_OCC), TIME_OCC=VALUES(TIME_OCC), Part_1_2=VALUES(Part_1_2), Mocodes=VALUES(Mocodes), Status=VALUES(Status), Status_Desc=VALUES(Status_Desc);
            """, (row['DR_NO'], row['Date Rptd'], row['DATE OCC'], row['TIME OCC'], row['AREA'], row['Rpt Dist No'], row['Part 1-2'], row['Mocodes'], row['Status'], row['Status Desc']))
            
            
            # Insert data into the Area table
            cursor.execute("""
            INSERT INTO Area (AREA, AREA_NAME)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE AREA_NAME=VALUES(AREA_NAME);
            """, (row['AREA'], row['AREA NAME']))
            count += 1

            # Insert data into the Victim table
            cursor.execute("""
            INSERT INTO Victim (DR_NO, Vict_Age, Vict_Sex, Vict_Descent)
            VALUES (%s, %s, %s, %s);
            """, (row['DR_NO'], row['Vict Age'], row['Vict Sex'], row['Vict Descent']))
            
            # Insert data into the PremiseWeapon table
            cursor.execute("""
            INSERT INTO PremiseWeapon (DR_NO, Premis_Cd, Premis_Desc, Weapon_Used_Cd, Weapon_Desc)
            VALUES (%s, %s, %s, %s, %s);
            """, (row['DR_NO'], row['Premis Cd'], row['Premis Desc'], row['Weapon Used Cd'], row['Weapon Desc']))
            
            # Insert data into the IncidentCrimeCode table
            cursor.execute("""
            INSERT INTO IncidentCrimeCode (DR_NO, Crime_Code, Cri_Cd1, Cri_Cd2, Cri_Cd3, Cri_Cd4)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['DR_NO'], row['Crm Cd'], row['Crm Cd 1'], row['Crm Cd 2'], row['Crm Cd 3'], row['Crm Cd 4']))

            cursor.execute("""
            INSERT INTO Location (DR_NO, Loc, Cro_Street, Lat, Lon)
            VALUES (%s, %s, %s, %s, %s);
            """, (row['DR_NO'], row['LOCATION'], row['Cross Street'], row['LAT'], row['LON']))

            conn.commit()
            print("Data inserted successfully for DR_NO:", row['DR_NO'])
        except Error as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
            
    return count


count = 0 #calculate the number of the total records
for area in data['AREA'].unique(): #choose AREA as the partition key
    df_area = data[data['AREA'] == area]
    conn = create_database_connection(area)
    if conn:
        create_tables(conn)
        count = insert_data(conn, df_area, count)
        conn.close()
        print(f"Processing complete for area: {area}")
        
print(count)
