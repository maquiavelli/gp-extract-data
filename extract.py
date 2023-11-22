import json
import os

from dotenv import load_dotenv
from enum import Enum
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from utils.logs import log_type,generate_log

#ENVIRONMENT CONFIG
load_dotenv()
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
FULL_LOAD_START_TIME =  os.getenv('PARAMS_FULL_LOAD_START_TIME')
FULL_LOAD_END_TIME =  os.getenv('PARAMS_FULL_LOAD_END_TIME')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

#JSON CONSTANTS
METRICS_KEY = "metrics"
DIMENSIONS_KEY = "dimensions"
TIMELINE_SPEC_KEY = "timelineSpec"
AGGREGATION_PERIOD_KEY = "aggregationPeriod"
START_TIME_KEY = "startTime"
END_TIME_KEY = "endTime"
PAGE_TOKEN_KEY = "pageToken"

class ReportType(Enum):
    CRASH_RATE = "crashRateMetricSet"
    SLOW_START_RATE = "slowStartRateMetricSet"
    ANR_RATE = "anrRateMetricSet"
    ERROR_COUNT = "errorCountMetricSet"

def get_scoped_credentials():
  scopes = ["https://www.googleapis.com/auth/playdeveloperreporting"]
  
  credentials_url = os.getenv('CREDENTIAL_URL_GOOGLE_PLAY_DEV_REPORTING')

  credentials = service_account.Credentials.from_service_account_file(credentials_url)
  return credentials.with_scopes(scopes)

def get_reporting_client():
  scoped_credentials = get_scoped_credentials()
  return build("playdeveloperreporting", 
                      "v1beta1", 
                      credentials=scoped_credentials, 
                      cache_discovery=False)

def get_body(structure_report, page_token):

    start_date = datetime.strptime(FULL_LOAD_START_TIME, "%Y-%m-%d")
    end_date = datetime.strptime(FULL_LOAD_END_TIME, "%Y-%m-%d")

    body = {
        METRICS_KEY: structure_report["metrics"],
        DIMENSIONS_KEY: structure_report["dimensions"],
        TIMELINE_SPEC_KEY: {
            AGGREGATION_PERIOD_KEY: "DAILY",
            START_TIME_KEY: {
                "day": start_date.day,
                "month": start_date.month,
                "year": start_date.year
            },
            END_TIME_KEY: {
                "day": end_date.day,
                "month": end_date.month,
                "year": end_date.year
            }
        },
        PAGE_TOKEN_KEY: page_token
    }
    
    return body

def get_report_method(structure_report):
    report_type = ReportType(structure_report["type"])

    reporting_user = get_reporting_client()

    if report_type == ReportType.CRASH_RATE:
        return reporting_user.vitals().crashrate()
    elif report_type == ReportType.SLOW_START_RATE:
        return reporting_user.vitals().slowstartrate()
    elif report_type == ReportType.ANR_RATE:
        return reporting_user.vitals().anrrate()
    elif report_type == ReportType.ERROR_COUNT:
        return reporting_user.vitals().errors().counts()

def writeJsonFile(data,file_name):
    raw_data_app_folder = f'{RAW_FOLDER}/{BUNDLE_APP}'
    if not os.path.exists(raw_data_app_folder):
        os.makedirs(raw_data_app_folder)
        
    file = f'{raw_data_app_folder}/{file_name}.json'
    with open(file, 'w', encoding='utf-8') as f:
          json.dump(data, f, ensure_ascii=False, indent=4)
    generate_log(log_type.FILE_CREATED,f"File {file} created!")

def read_json_file(json_file):
    f = open(json_file)
    return json.load(f)

def get_freshness_date(structure_report):
    dispatch_report = get_report_method(structure_report)
    data_response = dispatch_report.get(
                            name=f'apps/{BUNDLE_APP}/{structure_report["type"]}'
                            ).execute()
    return data_response

def main():
    reports = read_json_file("vitals-reports.json")
  
    for report in reports:  
        dispatch_report = get_report_method(report)
        
        should_repeat = True
        page_token = ''
        full_data_response = []
        page = 0

        while should_repeat:
            body = get_body(report,page_token)
            data_response = dispatch_report.query(
                            name=f'apps/{BUNDLE_APP}/{report["type"]}', 
                            body=body).execute()
            
            generate_log(log_type.DATA_READ,f"Retrieved page {page} of the query {report['type']}")
            
            if "rows" in data_response:
                full_data_response.extend(data_response["rows"])
            
            if "nextPageToken" in data_response:
                page_token = data_response["nextPageToken"]
            else:
                should_repeat = False
            
            page += 1
                
        writeJsonFile(full_data_response,report["fileName"])
    
    generate_log(log_type.PROCESS_FINISHED,f"Extract process finished!")
    
main()