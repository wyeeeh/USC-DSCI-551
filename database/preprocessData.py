

# Function to convert datetime format
# def convert_datetime(column):
#     # Assuming the original format is 'MM/DD/YYYY HH:MM:SS AM/PM'
#     return pd.to_datetime(data[column]).dt.strftime('%Y-%m-%d %H:%M:%S')


def process_data(df):
    import pandas as pd
    from datetime import datetime
    import os

    
    print (f"Data loaded with {len(df)} rows.")

    # Apply the conversion function to the columns
    df['Date Rptd'] = pd.to_datetime(df['Date Rptd'], format='%m/%d/%Y %H:%M:%S %p').dt.strftime('%Y-%m-%d')
    df['DATE OCC'] = pd.to_datetime(df['DATE OCC'], format='%m/%d/%Y %H:%M:%S %p').dt.strftime('%Y-%m-%d')
    df['TIME OCC'] = df['TIME OCC'].astype(str).str.zfill(4)
    df['TIME OCC'] = pd.to_datetime(df['TIME OCC'], format='%H%M').dt.strftime('%H:%M:%S')
    # df['Weapon Used Cd'] = df['Weapon Used Cd'].astype(int)
    # df['Crm Cd 1'] = df['Crm Cd 1'].astype(int)
    # df['Crm Cd 2'] = df['Crm Cd 2'].astype(int)
    # df['Crm Cd 3'] = df['Crm Cd 3'].astype(int)
    # df['Crm Cd 4'] = df['Crm Cd 4'].astype(int)

    print(f"Data processed with {len(df)} rows.")

    subset = df.dropna()
    for col in subset.columns:
        if subset is not None and subset[col].iloc[0] is not None:
            print(col, type(subset[col].iloc[0]).__name__, subset[col].iloc[0])
        else:
            print(type(df[col].iloc[0]).__name__, df[col].iloc[0])

    return df


if __name__ == "__main__":
    process_data()