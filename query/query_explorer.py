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


def main():
    print("Please enter a SELECT SQL query to execute across all Crime databases.")
    query = input("SQL Query: ")
    if not query.lower().strip().startswith('select'):
        print("Error: Only SELECT queries are allowed for security reasons.")
        return

    results = query_all_areas(query, HOST, USER, PASSWORD)
    if not results.empty:
        print(results)
    else:
        print("No data returned or an error occurred.")

if __name__ == "__main__":
    main()
