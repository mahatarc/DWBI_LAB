from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'D_RETAIL_LOCN_T '
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (LOCN_ID, RGN_KY, CNTRY_KY, LOCN_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
SELECT 
  S.ID AS LOCN_ID,                        
  R.RGN_KY,                    
  R.CNTRY_KY,                  
  S.STORE_DESC AS LOCN_DESC,   
  CURRENT_TIMESTAMP,           
  CURRENT_TIMESTAMP            
FROM {TEMP_DB_NAME}.STORE S
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_RGN_T R ON S.REGION_ID = R.RGN_ID;
"""

def load_location_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()