import json
from dotenv import load_dotenv
import os

from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

load_dotenv()
PARAMS_RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')

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
  body = {
          "metrics": 
              structureReport["metrics"],
          "dimensions": 
              structureReport["dimensions"],
          "timelineSpec": 
              {
              "aggregationPeriod": "DAILY",
              "startTime": {
                      "day": 1,
                      "month": 7,
                      "year": 2023
                  },
              "endTime": 
                  {
                      "day": 1,
                      "month": 10,
                      "year": 2023
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
    file = f'{PARAMS_RAW_FOLDER}/{fileName}.json'
    with open(file, 'w', encoding='utf-8') as f:
          json.dump(data, f, ensure_ascii=False, indent=4)
    print(f'{datetime.now()} : File {file} created!')

def main():
  reports = [
      {
          "app": "com.picpay",
          "type":"crashRateMetricSet",
          "metrics": ["crashRate","distinctUsers"],
          "dimensions": [],
          "fileName": "crash-rate-overview"
      },
      {
          "app": "com.picpay",
          "type":"crashRateMetricSet",
          "metrics": ["crashRate","distinctUsers"],
          "dimensions": ["versionCode"],
          "fileName": "crash-rate-by-version-code"
      },
      {
          "app": "com.picpay",
          "type":"anrRateMetricSet",
          "metrics": ["anrRate","distinctUsers"],
          "dimensions": [],
          "fileName": "anr-rate-overview"
      },
      {
          "app": "com.picpay",
          "type":"anrRateMetricSet",
          "metrics": ["anrRate","distinctUsers"],
          "dimensions": ["versionCode"],
          "fileName": "anr-rate-by-version-code"
      },
      {
          "app": "com.picpay",
          "type":"slowStartRateMetricSet",
          "metrics": ["slowStartRate"],
          "dimensions": ["startType"],
          "fileName": "slow-start-overview"
      },
      {
          "app": "com.picpay",
          "type":"slowStartRateMetricSet",
          "metrics": ["slowStartRate"],
          "dimensions": ["startType","versionCode"],
          "fileName": "slow-start-by-version-code"
      },
      {
          "app": "com.picpay",
          "type":"errorCountMetricSet",
          "metrics": ["errorReportCount","distinctUsers"],
          "dimensions": ["reportType"],
          "fileName": "error-count-overview"
      },
      {
          "app": "com.picpay",
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
                        name=f'apps/{report["app"]}/{report["type"]}', 
                        body=body).execute()
        
        print(f'{datetime.now()} : Retrieved page {page} of the query {report["type"]}')
        
        if "rows" in data_response:
            full_data_response.extend(data_response["rows"])
        
        if "nextPageToken" in data_response:
            page_token = data_response["nextPageToken"]
        else:
            should_repeat = False
        
        page += 1
            
    writeJsonFile(full_data_response,report["fileName"])
      
main()