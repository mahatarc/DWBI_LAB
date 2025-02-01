from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'D_RETAIL_SUB_CTGRY_T'
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (SUB_CTGRY_ID, CTGRY_KY, SUB_CTGRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
        SELECT 
            S.ID,
            C.CTGRY_KY, 
            S.SUBCATEGORY_DESC,
            CURRENT_TIMESTAMP as row_insrt_tms,
            CURRENT_TIMESTAMP as row_updt_tms
        FROM {TEMP_DB_NAME}.SUBCATEGORY S
        LEFT JOIN {TGT_DB_NAME}.D_RETAIL_CTGRY_T C
        ON S.CATEGORY_ID = C.CTGRY_ID
"""

def load_subcategory_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()