import json

class Variables:
    def __init__(self,variable_name):
        self.path = "C:/Users/achar/DWBI/config/config.cfg"
        self.name = variable_name

    def get_variable(self):
        with open(self.path,"r") as file:

            file_content = json.loads(file.read())
            print(file_content)
            return file_content[self.name]


var = Variables("DATABASE_NAME")


print(var.get_variable())