import json
import os

from datetime import datetime
from dotenv import load_dotenv
from utils.logs import log_type,generate_log
from utils.files import read_json_file, get_all_json_files_on_path,write_csv_file

load_dotenv()
# ENVIRONMENTS
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
DATASETS_FOLDER =  os.getenv('PARAMS_DATASETS_FOLDER')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

#CONSTANTS
PARAM_RAW_BUNDLE_PATH = os.path.join(RAW_FOLDER, BUNDLE_APP) 
      
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

def transform_report_data_to_event_list(report_data):
    return [transform_report_row(row) for row in report_data]

def main():
    try:
        json_path = PARAM_RAW_BUNDLE_PATH
        list_json_files = get_all_json_files_on_path(json_path)

        if list_json_files:
            for file in list_json_files:
                report_data = read_json_file(os.path.join(json_path, file))
                generate_log(log_type.FILE_READ, f"File read {file}")
                
                event_list = transform_report_data_to_event_list(report_data)
                clean_file_name, _ = os.path.splitext(file)
                
                dataset_path = DATASETS_FOLDER
                write_csv_file(dataset_path, clean_file_name, event_list)

            generate_log(log_type.PROCESS_FINISHED, "Transform process finished!")
        else:
            generate_log(log_type.FILE_NOT_FOUND, "Compatible files for transformation not found.")

    except FileNotFoundError as e:
        generate_log(log_type.FILE_NOT_FOUND, f"Files not found: {e}")

main()