import json
import os

from datetime import datetime
from dotenv import load_dotenv
from enum import Enum, auto 

from utils.logs import log_type,generate_log
from utils.files import read_json_file, get_all_json_files_on_path,write_csv_file

load_dotenv()
# ENVIRONMENTS
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
DATASETS_FOLDER =  os.getenv('PARAMS_DATASETS_FOLDER')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

#CONSTANTS

class ResourceType(Enum):
    METRIC = auto()
    REPORT = auto()
    
def transform_dimension(dimension):
    return {dimension["dimension"]: dimension["stringValue"]}

def transform_metric_row(row):
    new_event = {"date": f'{row["startTime"]["year"]}-{row["startTime"]["month"]}-{row["startTime"]["day"]}'}
    if "dimensions" in row:
        new_event.update({dimension["dimension"]: dimension["stringValue"] for dimension in row["dimensions"]})
    if "metrics" in row:
        new_event.update({metric["metric"]: metric["decimalValue"]["value"] for metric in row["metrics"]})
    return new_event

def transform_response_data_to_event_list(data,resource_type:ResourceType):
    print(resource_type)
    if resource_type == ResourceType.METRIC:
        return [transform_metric_row(row) for row in data]
    else:
        return [transform_report_row(row) for row in data]

def transform_report_row(row):
    return {
            "name" : row["name"],
            "type" :row["type"],
            "cause" :row["cause"],
            "location": row.get("location", ""),
            "errorReportCount" :row["errorReportCount"],
            "distinctUsers" :row["distinctUsers"],
            "lastErrorReportTime" :row["lastErrorReportTime"],
            "issueUri" : row["issueUri"],
            "firstOsVersion" : row["firstOsVersion"]["apiLevel"],
            "lastOsVersion" : row["lastOsVersion"]["apiLevel"],
            "firstAppVersion": row["firstAppVersion"]["versionCode"],
            "lastAppVersion": row["lastAppVersion"]["versionCode"],
            "distinctUsersPercent": row["distinctUsersPercent"]["value"]
        }

def get_resources_to_transform():
    
    base_path = os.path.join(RAW_FOLDER, BUNDLE_APP)
    
    metric_path = os.path.join(base_path,ResourceType.METRIC.name)
    report_path = os.path.join(base_path,ResourceType.REPORT.name)

    return [
        {
            "type": ResourceType.METRIC,
            "raw_path":metric_path
        },
        {
            "type": ResourceType.REPORT,
            "raw_path":report_path
        },
    ]


def transform_report_data_to_event_list(report_data):
    return [transform_metric_row(row) for row in metric_data]

def main():
    resources = get_resources_to_transform()
    
    for resource in resources:
        resource_type = resource["type"]
        resource_raw_path = resource["raw_path"]
        
        dataset_path = os.path.join(DATASETS_FOLDER,BUNDLE_APP,resource_type.name)
        
        json_files = get_all_json_files_on_path(resource_raw_path)
        if json_files:
            for file in json_files:
                metric_data = read_json_file(os.path.join(resource_raw_path, file))
                generate_log(log_type.FILE_READ, f"File read {file}")
                
                event_list = transform_response_data_to_event_list(metric_data,resource_type)
                clean_file_name, _ = os.path.splitext(file)
                
                
                write_csv_file(dataset_path, clean_file_name, event_list)

            generate_log(log_type.PROCESS_FINISHED, "Transform process finished!")
        else:
            generate_log(log_type.FILE_NOT_FOUND, "Compatible files for transformation not found.")

main()