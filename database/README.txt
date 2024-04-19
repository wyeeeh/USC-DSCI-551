#1 
crimedata.csv: raw data downloaded from https://data.lacity.org/Public-Safety/Domestic-Violence-Calls-from-2020-to-Present/qq59-f26t/about_data

#2
crimedata_processed.csv: The processed data will be uploaded to MySQL one by one.

#3
preprocessData.py: The original data is preprocessed, including modifying the data type to fit the MySQL data type and deleting redundant fields.

#4
uploadData.py: Read the data, build the database and table according to the partition field, and insert the data according to the partition field.
