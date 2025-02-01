from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'D_RETAIL_RGN_T'
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (RGN_ID,CNTRY_KY, RGN_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
SELECT R.ID,C.CNTRY_KY, R.REGION_DESC, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM {TEMP_DB_NAME}.REGION R
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_CNTRY_T C
ON R.COUNTRY_ID = C.CNTRY_ID;
"""

def load_region_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()