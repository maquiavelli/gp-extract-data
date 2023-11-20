import json
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from utils.logs import log_type,generate_log

load_dotenv()
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
DATASETS_FOLDER =  os.getenv('PARAMS_DATASETS_FOLDER')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

def get_all_json_files_on_folder(folder):
    path_to_json = folder
    return [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

def read_json_file(json_file):
    with open(json_file, 'r') as f:
        return json.load(f)
      
def transform_dimension(dimension):
    return {dimension["dimension"]: dimension["stringValue"]}

def transform_metric(metric):
    return {metric["metric"]: metric["decimalValue"]["value"]}

def transform_report_row(row):
    new_event = {"date": f'{row["startTime"]["year"]}-{row["startTime"]["month"]}-{row["startTime"]["day"]}'}
    if "dimensions" in row:
        new_event.update({dimension["dimension"]: dimension["stringValue"] for dimension in row["dimensions"]})
    if "metrics" in row:
        new_event.update({metric["metric"]: metric["decimalValue"]["value"] for metric in row["metrics"]})
    return new_event

def transform_report_data_to_event_list(reportData):
    return [transform_report_row(row) for row in reportData]

def write_csv_file(data, file_name):
    datasets_app_folder = os.path.join(DATASETS_FOLDER, BUNDLE_APP)
    if not os.path.exists(datasets_app_folder):
        os.makedirs(datasets_app_folder)

    csvFile = os.path.join(datasets_app_folder, f'{file_name}.csv')

    keys = data[0].keys()
    with open(csvFile, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    generate_log(log_type.FILE_CREATED, f"{csvFile} created!")

def main():
    try:
        json_files = get_all_json_files_on_folder(os.path.join(RAW_FOLDER, BUNDLE_APP))

        if json_files:
            for file in json_files:
                report_data = read_json_file(os.path.join(RAW_FOLDER, BUNDLE_APP, file))
                generate_log(log_type.FILE_READ, f"File read {file}")
                event_list = transform_report_data_to_event_list(report_data)

                clean_file_name, _ = os.path.splitext(file)
                write_csv_file(event_list, clean_file_name)

            generate_log(log_type.PROCESS_FINISHED, "Transform process finished!")
        else:
            generate_log(log_type.FILE_NOT_FOUND, "Compatible files for transformation not found.")

    except FileNotFoundError as e:
        generate_log(log_type.FILE_NOT_FOUND, f"Files not found: {e}")

main()