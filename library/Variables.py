import json
#import Logger
class Variables:
        
    @staticmethod
    def  get_variable(name):
        try:
            with open("D:\DWBI_Practical\config\config.cfg","r") as file:
                file_content = json.loads(file.read())
                return file_content[name]
        except Exception as e:
            print(f"[Error]:{e}")
