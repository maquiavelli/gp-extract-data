import json
from dotenv import load_dotenv
import os

from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from utils.logs import log_type,generate_log

load_dotenv()
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
FULL_LOAD_START_TIME =  os.getenv('PARAMS_FULL_LOAD_START_TIME')
FULL_LOAD_END_TIME =  os.getenv('PARAMS_FULL_LOAD_END_TIME')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

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

def get_body(structureReport,pageToken):
    
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(FULL_LOAD_START_TIME,date_format)
    end_date = datetime.strptime(FULL_LOAD_END_TIME,date_format)

    body = {
            "metrics": 
                structureReport["metrics"],
            "dimensions": 
                structureReport["dimensions"],
            "timelineSpec": 
                {
                "aggregationPeriod": "DAILY",
                "startTime": {
                        "day": start_date.day,
                        "month": start_date.month,
                        "year": start_date.year
                    },
                "endTime": 
                    {
                        "day": end_date.day,
                        "month": end_date.month,
                        "year": end_date.year
                    }
                },
            "pageToken": pageToken
            }
    
    return body
    
def get_report_method(structureReport):
  reportType = structureReport["type"]
  
  reporting_user = get_reporting_client()
  
  if reportType == "crashRateMetricSet":
      return reporting_user.vitals().crashrate()
  elif reportType == "slowStartRateMetricSet":
      return reporting_user.vitals().slowstartrate()
  elif reportType == "anrRateMetricSet":
      return reporting_user.vitals().anrrate()
  elif reportType == "errorCountMetricSet":
      return reporting_user.vitals().errors().counts()

def writeJsonFile(data,fileName):
    raw_data_app_folder = f'{RAW_FOLDER}/{BUNDLE_APP}'
    if not os.path.exists(raw_data_app_folder):
        os.makedirs(raw_data_app_folder)
        
    file = f'{raw_data_app_folder}/{fileName}.json'
    with open(file, 'w', encoding='utf-8') as f:
          json.dump(data, f, ensure_ascii=False, indent=4)
    generate_log(log_type.FILE_CREATED,f"File {file} created!")

def main():
    reports = [
        {
            "type":"crashRateMetricSet",
            "metrics": ["crashRate","distinctUsers"],
            "dimensions": [],
            "fileName": "crash-rate-overview"
        },
        {
            "type":"crashRateMetricSet",
            "metrics": ["crashRate","distinctUsers"],
            "dimensions": ["versionCode"],
            "fileName": "crash-rate-by-version-code"
        },
        {
            "type":"anrRateMetricSet",
            "metrics": ["anrRate","distinctUsers"],
            "dimensions": [],
            "fileName": "anr-rate-overview"
        },
        {
            "type":"anrRateMetricSet",
            "metrics": ["anrRate","distinctUsers"],
            "dimensions": ["versionCode"],
            "fileName": "anr-rate-by-version-code"
        },
        {
            "type":"slowStartRateMetricSet",
            "metrics": ["slowStartRate"],
            "dimensions": ["startType"],
            "fileName": "slow-start-overview"
        },
        {
            "type":"slowStartRateMetricSet",
            "metrics": ["slowStartRate"],
            "dimensions": ["startType","versionCode"],
            "fileName": "slow-start-by-version-code"
        },
        {
            "type":"errorCountMetricSet",
            "metrics": ["errorReportCount","distinctUsers"],
            "dimensions": ["reportType"],
            "fileName": "error-count-overview"
        },
        {
            "type":"errorCountMetricSet",
            "metrics": ["errorReportCount","distinctUsers"],
            "dimensions": ["reportType","versionCode"],
            "fileName": "error-count-by-version-code"
        }
    ]
  
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