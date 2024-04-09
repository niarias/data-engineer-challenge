import json
import os
current_directory = os.getcwd()

def load_json_file(path_config):
    file_path = os.path.join(current_directory, path_config)
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config


def array_to_colspecs(arr):
    start_pos = 0
    colspecs = []
    
    for width in arr:
        end_pos = start_pos + width 
        colspecs.append((start_pos, end_pos))
        start_pos = end_pos

    return colspecs

def verify_dict(dict_to_check,required_fields):
    for field in required_fields:
        if field not in dict_to_check:
            raise ValueError(f"Field {field} is required in the config file")
        
