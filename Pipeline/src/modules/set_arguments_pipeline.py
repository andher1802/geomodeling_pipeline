from utils import argument_handler
import datetime

def set_arguments_pipeline():
    arg_handler = argument_handler.Arguments_handler()
    arg_folder = argument_handler.Argument("folder")
    arg_folder.set_argument_type("String")
    arg_folder.set_argument_help_message("define the folder to write the result data (relative to the parent folder)")
    arg_folder.set_argument_default_value("data/klimatologie_temp")
    arg_handler.add_input_argument(arg_folder)
    arg_start_date = argument_handler.Argument("start_date")
    arg_start_date.set_argument_type("Date")
    arg_start_date.set_argument_help_message("define the start date to process format DD-MM-YYYY (hyphen sep)")
    arg_start_date.set_argument_default_value("01-01-2005")
    arg_handler.add_input_argument(arg_start_date)
    arg_end_date = argument_handler.Argument("end_date")
    arg_end_date.set_argument_type("Date")
    arg_end_date.set_argument_help_message("define the end date to process format DD-MM-YYYY (hyphen sep)")
    arg_end_date.set_argument_default_value("31-12-2014")
    arg_handler.add_input_argument(arg_end_date)    
    input_parameters_raw = arg_handler.get_input_arguments()
    input_parameters_checked = {}
    for arg in arg_handler.arguments:
        if arg.argument_type_name == "Date":
            try:
                if type(input_parameters_raw[arg.argument_name]) is datetime.datetime:
                    input_parameters_checked[arg.argument_name] = input_parameters_raw[arg.argument_name]
                elif type(input_parameters_raw[arg.argument_name]) is str:
                    temp_argument_type_date = datetime.datetime.strptime(input_parameters_raw[arg.argument_name], '%d-%m-%Y')
                    input_parameters_checked[arg.argument_name] = temp_argument_type_date
            except:
                print("wrong date format {0} date should match dd-mm-yyyy. {0} ommited".format(arg.argument_name))
        elif arg.argument_type_name == "Integer" or arg.argument_type_name == "Integer_no_zero":
            if not type(input_parameters_raw[arg.argument_name]) is int:
                raise TypeError("Only integers are allowed") 
        elif arg.argument_type_name == "Integer_no_zero" and input_parameters_raw[arg.argument_name] < 0:
            raise Exception("Sorry, no numbers below zero")
        else:
            input_parameters_checked[arg.argument_name] = input_parameters_raw[arg.argument_name]
    return input_parameters_checked