import mysql.connector
from mysql.connector import Error

HOST = 'localhost'
USER = 'root'
PASSWORD = 'dsci-551'
#DB = 'DSCI551Project'

#connet to the database
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

#