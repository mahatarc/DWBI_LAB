from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'D_RETAIL_CUSTOMER_T'
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (CUSTOMER_ID,CUSTOMER_FST_NM, CUSTOMER_MID_NM, CUSTOMER_LST_NM, CUSTOMER_ADDR, ROW_INSRT_TMS, ROW_UPDT_TMS)
SELECT 
    ID,
  CUSTOMER_FIRST_NAME, 
  CUSTOMER_MIDDLE_NAME, 
  CUSTOMER_LAST_NAME, 
  CUSTOMER_ADDRESS, 
  CURRENT_TIMESTAMP, 
  CURRENT_TIMESTAMP
FROM {TEMP_DB_NAME}.CUSTOMER;
"""

def load_customer_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()