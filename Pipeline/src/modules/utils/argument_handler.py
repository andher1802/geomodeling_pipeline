import datetime
import argparse
import enum
        
class Argument(object):
    argument_name = None
    argument_type = None
    argument_type_name = None
    argument_help_message = None
    argument_default_value = None
    types = {
        "Date":str,
        "Integer":int,
        "Integer_no_zero":int,
        "String":str    
    }

    def __init__(self, argument_name):
        self.argument_name = argument_name
    
    def set_argument_type(self, argument_type):
        self.argument_type_name = argument_type
        self.argument_type = self.types[argument_type]
        self.argument_default_value = self.__set_default_value_type()
    
    def set_argument_help_message(self, argument_help_message):
        self.argument_help_message = str(argument_help_message)
    
    def set_argument_default_value(self, argument_default_value):
        self.argument_default_value = self.__set_default_value_type(argument_default_value)

    def __set_default_value_type(self, argument_default_value = None):
        if self.argument_type_name == "Integer_no_zero":
            return 1 if argument_default_value < 1 | default_value is None else default_value
        elif self.argument_type_name == "Integer":
            return 1 if argument_default_value is None else int(argument_default_value)
        elif self.argument_type_name == "Date":
            return datetime.datetime.strftime(datetime.date.today(), '%d-%m-%Y') if argument_default_value is None\
            else datetime.datetime.strptime(argument_default_value, '%d-%m-%Y')
        elif self.argument_type_name == "String":
            return '' if argument_default_value is None else str(argument_default_value)

        
class Arguments_handler(object):
    arguments = []
    parser = None
    
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        
    def add_input_argument(self, argument):
        self.parser.add_argument( 
            "--"+argument.argument_name,
            dest = argument.argument_name,
            default = argument.argument_default_value,
            help = argument.argument_help_message,
            type = argument.argument_type  
        )
        self.arguments.append(argument)

    def get_input_arguments(self):
        all_arguments = self.parser.parse_args()
        input_parameters = vars(all_arguments)
        return input_parameters