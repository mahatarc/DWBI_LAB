from library.Database import Database
from library.Variables import Variables
import os

TABLE_NAME = 'D_RETAIL_CTGRY_T'
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')

truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""

insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (CTGRY_ID, CTGRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
SELECT 
    ID, 
    CATEGORY_DESC,
    CURRENT_TIMESTAMP as row_insrt_tms,
    CURRENT_TIMESTAMP as row_updt_tms
FROM {TEMP_DB_NAME}.CATEGORY;
"""

def load_category_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = None  # Initialize db variable to avoid reference errors in finally block

    try:
        db = Database(file_name)
        print("Connected to the database.")

        # Truncate table
        print(f"Truncating table {TGT_DB_NAME}.{TABLE_NAME}...")
        db.execute_query(truncate_query)
        print("Table truncated successfully.")

        # Insert data
        print(f"Inserting data into {TGT_DB_NAME}.{TABLE_NAME}...")
        db.execute_query(insert_query)
        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        if db:
            try:
                db.disconnect()
                print("Disconnected from the database.")
            except Exception as e:
                print(f"Failed to disconnect: {e}")

