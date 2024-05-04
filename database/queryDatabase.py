import pandas as pd
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

# # check key
# for key, value in db_config.items():
#     st.write(f"{key}: {value}")

# -- Query data --
def execute_query(query, db_config, db_name=None):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}', echo=False)
        with engine.connect() as conn:
            if not db_name:
                results = conn.execute(text(query))
                conn.commit()
            else:
                # print(f"Connected to database {db_name} successfully")
                conn.execute(text(f"""USE {db_name};"""))
                # print(f"Connected to database {db_name}")
                conn.commit()
                results = conn.execute(text(query))
            return results.all()
    except Error as err:
        print(f"Error: '{err}'")
        return None
    
def select_query(query, db_config, db_name):
    encoded_password = urllib.parse.quote_plus(db_config["password"])
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}'+f'@{db_config["host"]}/{db_name}', echo=False)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
    except Error as err:
        print(f"Error: '{err}'")
        return None