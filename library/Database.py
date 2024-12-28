import mysql.connector
import pandas as pd
from Variables import Variables
class Database:
    def __init__(self,database):
        try:
            self.connection = mysql.connector.connect(
                host = Variables("host").get_variable(),
                port = Variables("port").get_variable(),
                user = Variables("user").get_variable(),
                password = Variables("password").get_variable(),
                database= database
            )
            if self.connection.is_connected():
                print("Successfully connected to MySQL!")
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL: {e}")


    def execute_query(self,select_query):
        df= pd.read_sql(select_query,self.connection)
        return df
    
    def disconnect(self):
        pass

    def fetch(self):
        pass
      
db= Database("DWBI")
table = db.execute_query("SELECT * FROM STUDENT")
print(table.head())