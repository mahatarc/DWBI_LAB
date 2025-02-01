from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'D_RETAIL_PDT_T'
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (PDT_ID,SUB_CTGRY_KY,CTGRY_KY,PDT_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
    SELECT 
        P.ID,
        S.SUB_CTGRY_KY,        
        S.CTGRY_KY,
        P.PRODUCT_DESC,
        CURRENT_TIMESTAMP, 
        CURRENT_TIMESTAMP
FROM {TEMP_DB_NAME}.PRODUCT P
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_SUB_CTGRY_T S 
  ON P.SUBCATEGORY_ID = S.SUB_CTGRY_ID;
"""

def load_product_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()