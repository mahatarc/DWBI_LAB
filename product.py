import os
from library.Database import Database

file_name = os.path.basename(__file__).split('-')[0]

try:
    db = Database(file_name)
    print("Connected to the database.")

    # Extract data from OLTP to CSV
    df = db.ext_to_file("Products")

    # Load CSV data into OLAP staging table
    stg_table = db.load_to_stg("D:/DWBI_Practical/products.csv")
    print("Loaded in the staging table.")
    
except Exception as e:
     print(f"An error occurred: {e}") 
finally:
    try:
        db.disconnect()
    except Exception as e:
        print(f"Failed to disconnect: {e}")
    
# OLAP _<Fname> STG/TMP/TGT
# OLTP_ <Fname> SRC-> Product




