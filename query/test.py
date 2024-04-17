from db_operations import query_all_areas

# Database credentials
HOST = 'localhost'
USER = 'root'
PASSWORD = 'lily1221'


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

