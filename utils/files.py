import json
import os
import csv

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

def get_all_json_files_on_path(path):
    return [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]

def write_csv_file(path, file_name, data):
    path_to_write_file = path
    if not os.path.exists(path_to_write_file):
        os.makedirs(path_to_write_file)

    csvFile = os.path.join(path_to_write_file, f'{file_name}.csv')

    keys = data[0].keys()
    with open(csvFile, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    generate_log(log_type.FILE_CREATED, f"{csvFile} created!")