import json
#import Logger
class Variables:
    #def __init__(self,variable_name):
     #   self.path = "D:\DWBI_Practical\config\config.cfg"
      #  self.name = variable_name
        
    @staticmethod
    def  get_variable(name):
        try:
            with open("D:\DWBI_Practical\config\config.cfg","r") as file:
                file_content = json.loads(file.read())
                return file_content[name]
        except Exception as e:
            print(f"[Error]:{e}")
