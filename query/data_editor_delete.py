import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection configuration
db_config = pd.read_json('../USC-DSCI-551/key.json', typ = 'series').to_dict()



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
  

def delete_record(data):
    """Deletes records from the corresponding databases and tables, ensuring no foreign key constraint fails."""
    if 'AREA' not in data:
        print("Error: AREA field is missing in the provided data.")
        return

    area = data['AREA']
    conn = create_database_connection(area)
    if conn is None:
        return  # If the connection fails, return

    cursor = conn.cursor()
    try:
        # Ensure data dictionary values are None if they are NaN (pandas handling)
        data = {k: v if pd.notna(v) else None for k, v in data.items()}

        # Delete records from dependent tables first
        # Delete a record from the Victim table
        cursor.execute("""
        DELETE FROM Victim WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Victim")

        # Delete a record from the PremiseWeapon table
        cursor.execute("""
        DELETE FROM PremiseWeapon WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from PremiseWeapon")

        # Delete a record from the IncidentCrimeCode table
        cursor.execute("""
        DELETE FROM IncidentCrimeCode WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from IncidentCrimeCode")

        # Delete a record from the Location table
        cursor.execute("""
        DELETE FROM Location WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from Location")

        # Finally, delete the record from the CrimeIncident table
        cursor.execute("""
        DELETE FROM CrimeIncident WHERE DR_NO = %s;
        """, (data['DR_NO'],))
        print("Record deleted successfully from CrimeIncident")

        conn.commit()
    except Error as e:
        print(f"Error deleting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# 测试删除功能
delete_data = {
    'DR_NO': 'AA123456',
    'AREA': '1'
}

delete_record(delete_data)

