# Import the required functions
from db_operations import execute_query, execute_query_all_areas

def main():
    while True:
        # Get user input
        area_code = input("Enter the area code (1-21) or 'all' to query all areas: ")
        query = input("Enter your SQL query: ")

        # Check if querying all areas
        if area_code.lower() == 'all':
            results = execute_query_all_areas(query)
        else:
            try:
                area_code = int(area_code)  # Convert to integer
                if 1 <= area_code <= 21:
                    results = execute_query(area_code, query)
                else:
                    raise ValueError
            except ValueError:
                print("Invalid area code. Please enter a number between 1 and 21.")
                continue

        # Print results
        print("Query results:")
        print(results)

        # Ask if the user wants to continue
        cont = input("Do you want to run another query? (yes/no): ")
        if cont.lower() != 'yes':
            break

if __name__ == "__main__":
    main()
