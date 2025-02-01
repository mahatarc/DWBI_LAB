from library.Database import Database
from library.Logger import Logger
from library.Variables import Variables
import os
TABLE_NAME = 'F_RETAIL_SLS_T '
TGT_DB_NAME = Variables.get_variable('DB_TGT')
TEMP_DB_NAME = Variables.get_variable('TEMP_DB')
truncate_query = f"""
TRUNCATE TABLE {TGT_DB_NAME}.{TABLE_NAME};
"""
insert_query = f"""
INSERT INTO {TGT_DB_NAME}.{TABLE_NAME} (SLS_ID,LOCN_KY,DT_KY, PDT_KY, CUSTOMER_KY, TRANSACTION_TIME, QTY, AMT, DSCNT, ROW_INSRT_TMS, ROW_UPDT_TMS)
SELECT 
  S.ID,            
  L.LOCN_KY,
  CA.DAY_KEY,
  P.PDT_KY,                  
  C.CUSTOMER_KY,             
  S.TRANSACTION_TIME,        
  S.QUANTITY AS QTY,         
  S.AMOUNT AS AMT,           
  S.DISCOUNT AS DSCNT,       
  CURRENT_TIMESTAMP,         
  CURRENT_TIMESTAMP          
FROM {TEMP_DB_NAME}.SALES S
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_PDT_T P ON S.PRODUCT_ID = P.PDT_ID  
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_CUSTOMER_T C ON S.CUSTOMER_ID = C.CUSTOMER_ID
LEFT JOIN {TGT_DB_NAME}.D_RETAIL_LOCN_T L ON S.STORE_ID = L.LOCN_ID
LEFT JOIN {TGT_DB_NAME}.DIM_CALENDAR CA ON DATE(S.TRANSACTION_TIME) = CA.DATE;
"""

def load_sales_to_tgt():
    file_name = os.path.basename(__file__).split('-')[0]
    db = Database(file_name)
    db.execute_query(truncate_query)
    db.execute_query(insert_query)
    db.disconnect()