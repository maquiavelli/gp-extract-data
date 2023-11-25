import json
import os

from .logs import log_type,generate_log

def write_json_file(path,file_name,data):
    raw_data_app_folder = path
    if not os.path.exists(raw_data_app_folder):
        os.makedirs(raw_data_app_folder)
        
    file = f'{path}/{file_name}.json'
    with open(file, 'w', encoding='utf-8') as f:
          json.dump(data, f, ensure_ascii=False, indent=4)
    generate_log(log_type.FILE_CREATED,f"File {file} created!")

def read_json_file(json_file):
    f = open(json_file)
    return json.load(f)
