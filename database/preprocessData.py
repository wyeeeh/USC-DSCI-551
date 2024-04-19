import pandas as pd
from datetime import datetime
import os

# Function to convert datetime format
def convert_datetime(column):
    # Assuming the original format is 'MM/DD/YYYY HH:MM:SS AM/PM'
    return pd.to_datetime(data[column]).dt.strftime('%Y-%m-%d %H:%M:%S')

# -- data repo --
data_dir = os.path.join(os.getcwd(), "data")
file_path = "domestic_violence_calls.csv"
data = pd.read_csv(os.path.join(data_dir,file_path))

# Apply the conversion function to the columns
data['Date Rptd'] = convert_datetime('Date Rptd')
data['DATE OCC'] = convert_datetime('DATE OCC')

# Remove the redundant column Crm Cd 1 because it is the same as Crm Cd.
#data.drop(columns=['Crm Cd 1'], inplace=True)
    

# Print the processed data
print(data.head())

# Save the processed DataFrame to a new CSV file
output_file_path = 'crimedata_processed.csv'
data.to_csv(os.path.join(data_dir,output_file_path), index=False)

print(f"Data saved to {output_file_path}")


