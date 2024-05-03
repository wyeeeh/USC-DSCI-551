# main.py

# Database credentials
HOST = 'localhost'
USER = 'root'
PASSWORD = 'lily1221'

from query.data_exlorer import collect_user_input, build_query
from query.query_explorer import execute_query, query_all_areas

def main():
    user_inputs = collect_user_input()
    query = build_query(user_inputs)
    results = query_all_areas(query, HOST, USER, PASSWORD)
    if not results.empty:
        print(results)
    else:
        print("No data returned or an error occurred.")

if __name__ == "__main__":
    main()
