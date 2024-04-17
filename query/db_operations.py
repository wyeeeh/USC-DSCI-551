import mysql.connector
from mysql.connector import Error
import pandas as pd

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
        #print(f"Connection to {db_name} successful")
        return connection
    except Error as e:
        print(f"Failed to connect to {db_name}: {e}")
        return None

def execute_query(connection, query):
    """Execute a query and return results as a DataFrame."""
    try:
        data_frame = pd.read_sql_query(query, connection)
        return data_frame
    except Error as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def query_all_areas(query, host_name, user_name, user_password):
    """Execute a query across all 21 Crime databases and collect results into a DataFrame."""
    all_results = pd.DataFrame()
    for area_code in range(1, 22):
        connection = create_db_connection(area_code, host_name, user_name, user_password)
        if connection is not None:
            result = execute_query(connection, query)
            result['Area_Code'] = area_code  # Add an Area_Code column to distinguish results
            all_results = pd.concat([all_results, result], ignore_index=True)
            connection.close()
    return all_results
