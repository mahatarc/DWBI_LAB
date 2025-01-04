import logging
from Variables import Variables
import os
import  datetime

class Logger:
    def __init__(self,file_name):
        current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.file_name = f"{file_name}_{current_ts}.log"
        self.log_path= os.path.join(Variables.get_variable('log_path'),self.file_name)
        
        self.logger = logging.getLogger(self.file_name)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(self.log_path)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(file_handler)
        
    def log_info(self,msg):
        self.logger.info(msg)
        
    def log_error(self,msg):
        self.logger.error(msg)
        