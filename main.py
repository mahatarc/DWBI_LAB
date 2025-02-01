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
        db.stg_to_temp(table_name)
    
except Exception as e:
    print(f"An error occurred: {e}") 

finally:
    try:
        db.disconnect()
    except Exception as e:
        print(f"Failed to disconnect: {e}")
 

 
#  try :
#      temp_query = f"""
#       INSERT INTO DW_STG.Product
#       SELECT 
#         product_id,
#         product_name,
#         product_desc,
#         product_price
#         FROM DW_STG.Product
#      """ 
#      db.execute_query()


#      tgt_query = """
#      INSERT INTO DW_STG.Product
#       SELECT 
#       sha_256(product_id,product_name,product_price) as product_key
#         product_id,
#         product_name,
#         product_desc,
#         product_price
#         Current_datetime() as rcd_ins_ts,
#         current_datetime() as rcd_upd_ts
#         FROM DW_STG.Product
#      """   
#         merge_query_scd1 = f"""
#          MERGE INTO DW_TGT.product
#          USING DW_TMP.product as TMP
#          ON TGT.product_id = TMP.product_id
#          WHEN MATCHED AND TGT.product_name <> TGT.product_name
#          THEN 
#          TGT.product_name = TMP.product_name 
#          TGT.rcd_upd_ts = current_timestamp()
         
#          WHEN NOT MATCHED  
#          INSERT into dw_tgt.product
#          select 
#          generate_product_key,
#          product_id as product_id,
#          product_name,
#          product_price,
#          product_desc,
#          current_timestamp() as rcd_ins_ts,
#          current_timesatmp() as rcd_upd_ts
#          from dw_stg.product
#         """
#         scd2 = f"""
#         MERGE INTO DW_TGT.product
#          USING DW_TMP.product as TMP
#          ON TGT.product_id = TMP.product_id
#          WHEN MATCHED AND TGT.product_name <> TGT.product_name (SCD1)
#          THEN 
#             tgt.eff_end_data = current_timestamp() -1
#             tgt.active_flag = false
#             tgt.rcd_upd_ts = current_timestamp()
         
#          WHEN NOT MATCHED  and tgt.product_name=tmp.product_name
#          INSERT into dw_tgt.product
#          select 
#          generate_product_key,
#          product_id as product_id,
#          product_name,
#          product_price,
#          product_desc,
#          current_timestamp() as rcd_ins_ts,
#          current_timesatmp() as rcd_upd_ts,
#          current_date() as eff_start_date,
#          '9999-12-31' as eff_end_date,
#          TRUE as active_flag
#          from dw_stg.product
         
#          INSERT INTO DW_TGT.product tgt 
#          select * from dw_tmp.product tmp
#          where tmp.product_id
#          NOT IN 
         
#              select product
         
#         """
# # except: