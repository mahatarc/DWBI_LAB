import json

class Variables:
    def __init__(self,variable_name):
        self.path = "D:\DWBI_Practical\config\config.cfg"
        self.name = variable_name

    def get_variable(self):
        with open(self.path,"r") as file:

            file_content = json.loads(file.read())
            return file_content[self.name]


var = Variables("database")


print(var.get_variable())