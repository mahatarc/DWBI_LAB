import mysql.connector
import pandas as pd
from .Variables import Variables
from .Logger import Logger

class Database:

    def __init__(self,file_name): # datatype definition
        try:
            self.logger = Logger(file_name)
            self.connection = mysql.connector.connect(
                host = Variables.get_variable('host'),
                port = Variables.get_variable('port'),
                user = Variables.get_variable('user'),
                password = Variables.get_variable('password'),
                #database= Variables.get_variable('SRC_DB'),
                allow_local_infile=True  # Enable LOCAL INFILE
            )
            self.cursor=self.connection.cursor()
            if self.connection.is_connected():
                self.logger.log_info("Successfully connected to MySQL!")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error connecting to MySQL: {e}")


    def execute_query(self,select_query):  #for select only
        self.logger.log_info(select_query)
        self.cursor.execute(select_query)
    
    def ext_to_file(self,table_name):
        #OLTP database
        select_query= f"""
              SELECT * from {Variables.get_variable('SRC_DB')}.{table_name}
        """ 
             
        self.execute_query(select_query)
        data= self.fetchall()
         # Get the column names from the cursor
        columns = [desc[0] for desc in self.cursor.description]
        # Convert the fetched data to a pandas DataFrame with column names
        df = pd.DataFrame(data, columns=columns)
        df.to_csv("data_output.csv", index=False)
        self.logger.log_info("Data exported to data_output.csv")
        return df
        
    def fetchall(self):
        try:
            results = self.cursor.fetchall()
            self.logger.log_info("Fetched all results.")
            return results
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error fetching results: {e}")
            raise
    
    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.log_info("Connection closed.")
        
    def load_to_stg(self,csv_file_path):
        try:
            #self.cursor.execute("USE OLAP_ARCHA;")
            
            select_query = f"""
            LOAD DATA LOCAL INFILE '{csv_file_path}'
            INTO TABLE OLAP_ARCHA.Products_Staging
            FIELDS TERMINATED BY ','  -- CSV delimiter
            ENCLOSED BY '"'           -- Enclose values in double quotes (if applicable)
            LINES TERMINATED BY '\n'  -- Line delimiter
            IGNORE 1 LINES           -- Skip the header row
            (product_id, product_name, price, stock_quantity, description, created_at);
            """
            self.cursor.execute(select_query)
            self.commit()
            self.logger.log_info("Data loaded successfully into the staging table.")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error loading data: {e}")
    
    def commit(self):
        self.connection.commit()

    def fetch(self):
        pass
      

