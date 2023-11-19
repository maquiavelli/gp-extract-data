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

def read_json_file(jsonFile):
    f = open(jsonFile)
    return json.load(f)
      
def transform_report_data_to_event_list(reportData):
  event_list = []
  for row in reportData:
      new_event = {"date": f'{row["startTime"]["year"]}-{row["startTime"]["month"]}-{row["startTime"]["day"]}' }
      if "dimensions" in row:
        for dimension in row["dimensions"]:
            new_event[dimension["dimension"]] = dimension["stringValue"]
      if "metrics" in row:
        for metric in row["metrics"]:
            new_event[metric["metric"]] = metric["decimalValue"]["value"]
      event_list.append(new_event)
  return event_list

def write_csv_file(data,fileName):
    datasets_app_folder = f'{DATASETS_FOLDER}/{BUNDLE_APP}'
    if not os.path.exists(datasets_app_folder):
        os.makedirs(datasets_app_folder)
        
    csvFile = f'{datasets_app_folder}/{fileName}.csv'
    
    keys = data[0].keys()
    with open(csvFile, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
        
    generate_log(log_type.FILE_CREATED,f"{csvFile} created!")

def main():
    try:
        jsonFiles = get_all_json_files_on_folder(f'{RAW_FOLDER}/{BUNDLE_APP}')
        
        if len(jsonFiles) > 0:
            for file in jsonFiles:
                reportData = read_json_file(f'{RAW_FOLDER}/{BUNDLE_APP}/{file}')
                generate_log(log_type.FILE_READ,f"File read {file}")
                eventList = transform_report_data_to_event_list(reportData)

                cleanFileName = file.replace('.json','')        
                write_csv_file(eventList,cleanFileName)

            generate_log(log_type.PROCESS_FINISHED,f"Transform process finished!")
        else:
            generate_log(log_type.FILE_NOT_FOUND,f"Compatible files for transformation not found.")
            
    except FileNotFoundError :
        generate_log(log_type.FILE_NOT_FOUND,f"Compatible files for transformation not found.")
main()