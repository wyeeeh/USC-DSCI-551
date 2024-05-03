import mysql.connector
from mysql.connector import Error
import pandas as pd

# Database credentials
HOST = 'localhost'
USER = 'root'
PASSWORD = 'lily1221'

def create_db_connection(area_code, host_name, user_name, user_password):
    """Create a database connection."""
    db_name = f"Crime_{area_code}"
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        return connection
    except Error as e:
        print(f"Failed to connect to {db_name}: {e}")
        return None


def collect_user_input():
    """Prompt the user for query parameters and return as a dictionary."""
    print("Enter query parameters (leave blank to include all records for that field).")
    area = input("AREA codes (comma-separated, e.g., 1,2,21): ")
    date_range = input("DATE OCC range (YYYY-MM-DD to YYYY-MM-DD): ")
    time_range = input("Time OCC range (HH:MM to HH:MM): ")
    age_range = input("Victim Age range (e.g., 10 to 35): ")

    return {
        'AREA': area,
        'DATE': date_range,
        'TIME': time_range,
        'AGE': age_range
    }

def build_query(params):
    """Construct and return an SQL query based on user inputs."""
    conditions = []
    if params['AREA']:
        areas = params['AREA'].replace(" ", "").split(',')
        conditions.append(f"AREA IN ({', '.join(areas)})")
    if params['DATE']:
        dates = params['DATE'].split('to')
        conditions.append(f"`DATE_OCC` BETWEEN '{dates[0].strip()}' AND '{dates[1].strip()}'")
    if params['TIME']:
        times = params['TIME'].split('to')
        conditions.append(f"`TIME_OCC` BETWEEN '{times[0].strip()}' AND '{times[1].strip()}'")
    if params['AGE']:
        ages = params['AGE'].split('to')
        conditions.append(f"`Vict_Age` BETWEEN {ages[0].strip()} AND {ages[1].strip()}")

    query = "SELECT * FROM CrimeIncident JOIN Victim ON CrimeIncident.DR_NO = Victim.DR_NO"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    return query


def execute_query(connection, query):
    """Execute a query and return results as a DataFrame."""
    try:
        if connection is not None:
            data_frame = pd.read_sql(query, connection)
            connection.close()
            return data_frame
    except Error as e:
        print(f"Error '{e}' occurred")
        return pd.DataFrame()

def main():
    # Collect user inputs
    params = collect_user_input()
    # Build the SQL query based on user inputs
    query = build_query(params)
    # Execute the query on specified databases
    all_results = pd.DataFrame()
    if params['AREA']:
        areas = params['AREA'].replace(" ", "").split(',')
        for area_code in areas:
            connection = create_db_connection(area_code, HOST, USER, PASSWORD)
            if connection:
                result = execute_query(connection, query)
                result['Area_Code'] = area_code  # Add an Area_Code column to distinguish results
                all_results = pd.concat([all_results, result], ignore_index=True)
    else:
        # If no specific area is mentioned, query all areas from 1 to 21
        for area_code in range(1, 22):
            connection = create_db_connection(str(area_code), HOST, USER, PASSWORD)
            if connection:
                result = execute_query(connection, query)
                result['Area_Code'] = str(area_code)
                all_results = pd.concat([all_results, result], ignore_index=True)

    # Print all results
    print(all_results)

if __name__ == "__main__":
    main()
