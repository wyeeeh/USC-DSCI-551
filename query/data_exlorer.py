# import mysql.connector
# from mysql.connector import Error

# HOST = 'localhost'
# USER = 'root'
# PASSWORD = 'dsci-551'
# #DB = 'DSCI551Project'

# #connet to the database
# def create_db_connection(host_name, user_name, user_password, db_name):
#     connection = None
#     try:
#         connection = mysql.connector.connect(
#             host=host_name,
#             user=user_name,
#             password=user_password,
#             database=db_name
#         )
#         print("MySQL Database connection successful")
#     except Error as e:
#         print(f"The error '{e}' occurred")
#     return connection

# data_explorer.py

def collect_user_input():
    """Prompt the user for query parameters and return as a dictionary."""
    print("Enter query parameters (leave blank to include all records for that field).")
    area = input("AREA codes (comma-separated, e.g., 1,2,21): ")
    date_range = input("DATE range (YYYY-MM-DD - YYYY-MM-DD): ")
    time_range = input("Time range (HH:MM - HH:MM): ")
    age_range = input("Victim Age range (e.g., 10 - 35): ")

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
        dates = params['DATE'].split('-')
        conditions.append(f"DATE BETWEEN '{dates[0].strip()}' AND '{dates[1].strip()}'")
    if params['TIME']:
        times = params['TIME'].split('-')
        conditions.append(f"TIME BETWEEN '{times[0].strip()}' AND '{times[1].strip()}'")
    if params['AGE']:
        ages = params['AGE'].split('-')
        conditions.append(f"`Vict Age` BETWEEN {ages[0].strip()} AND {ages[1].strip()}")

    query = "SELECT * FROM CrimeIncident"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    return query
