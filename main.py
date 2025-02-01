import os
from library.Database import Database

file_name = os.path.basename(__file__).split('-')[0]

try:
    db = Database(file_name)
    print("Connected to the database.")

    # Fetch all table names dynamically
    table_names = db.get_table_names()

    # Extract data from OLTP to CSV for each table
    for table_name in table_names:
        db.ext_to_file(table_name)
        print(f"Data extracted to CSV for table: {table_name}")
        
        # Load CSV data into OLAP staging table for each table
        db.load_to_stg(table_name)
        print(f"Loaded table '{table_name}' into the staging table.")
    db.disconnect()   
    
    db = Database(file_name)
    
    for table_name in table_names:    
        db.stg_to_temp(table_name)
        print(f"Loaded table '{table_name}' into the temp table.")
    db.execute_query("SET FOREIGN_KEY_CHECKS = 1;")
    db.disconnect()
    
    
except Exception as e:
    print(f"An error occurred: {e}") 

finally:
    try:
        db.disconnect()
    except Exception as e:
        print(f"Failed to disconnect: {e}")
 