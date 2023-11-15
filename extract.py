import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

def get_scoped_credentials():
  SCOPES = ["https://www.googleapis.com/auth/playdeveloperreporting"]

  credentials = service_account.Credentials.from_service_account_file('credentials.json')
  return credentials.with_scopes(SCOPES)

def get_reporting_client():
  scoped_credentials = get_scoped_credentials()
  return build("playdeveloperreporting", 
                      "v1beta1", 
                      credentials=scoped_credentials, 
                      cache_discovery=False)

def get_body(structureReport):
  body = {
          "metrics": 
              structureReport["metrics"],
          "dimensions": 
              structureReport["dimensions"],
          "timelineSpec": 
              {
              "aggregationPeriod": "DAILY",
              "endTime": 
                  {
                      "day": 11,
                      "month": 11,
                      "year": 2023
                  },
              "startTime": {
                      "day": 10,
                      "month": 11,
                      "year": 2023
                  }
              }
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
  with open(f'raw_data/{fileName}.json', 'w', encoding='utf-8') as f:
          json.dump(data, f, ensure_ascii=False, indent=4)

def main():
  reports = [
      {
          "app": "com.picpay",
          "type":"crashRateMetricSet",
          "metrics": ["crashRate","distinctUsers"],
          "dimensions": []
      },
      {
          "app": "com.picpay",
          "type":"anrRateMetricSet",
          "metrics": ["anrRate","distinctUsers"],
          "dimensions": []
      },
      {
          "app": "com.picpay",
          "type":"slowStartRateMetricSet",
          "metrics": ["slowStartRate"],
          "dimensions": ["startType"]
      },
      {
          "app": "com.picpay",
          "type":"errorCountMetricSet",
          "metrics": ["errorReportCount","distinctUsers"],
          "dimensions": ["reportType"]
      }
  ]
  
  for report in reports:
      body = get_body(report)
      
      dispatchReport = get_report_method(report)

      dataReponse = dispatchReport.query(
                      name=f'apps/{report["app"]}/{report["type"]}', 
                      body=body).execute()
      
      writeJsonFile(dataReponse,report["type"])

  # map_metrics(dataReponse, "crashRate")

main()