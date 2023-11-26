import os
from urllib.parse import urlencode

from dotenv import load_dotenv
from enum import Enum
from datetime import datetime,timedelta,timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials


from utils.logs import log_type,generate_log
from utils.files import read_json_file,write_json_file


#ENVIRONMENT CONFIG
load_dotenv()
RAW_FOLDER =  os.getenv('PARAMS_RAW_FOLDER')
START_TIME =  os.getenv('PARAMS_START_TIME')
BUNDLE_APP = os.getenv("PARAMS_BUNDLE_APP")

#JSON CONSTANTS
METRICS_KEY = "metrics"
DIMENSIONS_KEY = "dimensions"
TIMELINE_SPEC_KEY = "timelineSpec"
AGGREGATION_PERIOD_KEY = "aggregationPeriod"
START_TIME_KEY = "startTime"
END_TIME_KEY = "endTime"
PAGE_TOKEN_KEY = "pageToken"
INTERVAL = "interval"
FILTER = "filter"
PAGE_SIZE = "pageSize"

#PARAMS
AGGREGATION_DEFAULT = "DAILY"
RESOURCES_FILE ="resources-config.json"

#FIXED CONFIG TO GET REPORT
REPORT_PAGE_SIZE = 1000

class ResourceMethod(Enum):
    CRASH_RATE = "crashRateMetricSet"
    SLOW_START_RATE = "slowStartRateMetricSet"
    ANR_RATE = "anrRateMetricSet"
    ERROR_COUNT = "errorCountMetricSet"
    ERROR_ISSUES = "errorIssues"
    ERROR_REPORTS = "errorReports"

def get_scoped_credentials():
  scopes = ["https://www.googleapis.com/auth/playdeveloperreporting"]
  
  credentials_url = os.getenv('CREDENTIAL_URL_GOOGLE_PLAY_DEV_REPORTING')

  credentials = service_account.Credentials.from_service_account_file(credentials_url)
  return credentials.with_scopes(scopes)

def get_resource_client():
  scoped_credentials = get_scoped_credentials()
  return build("playdeveloperreporting", 
                      "v1beta1", 
                      credentials=scoped_credentials, 
                      cache_discovery=False)

def get_metric_body(resource, page_token,start_date, end_date):

    body = {
        METRICS_KEY: resource["metrics"],
        DIMENSIONS_KEY: resource["dimensions"],
        TIMELINE_SPEC_KEY: {
            AGGREGATION_PERIOD_KEY: AGGREGATION_DEFAULT,
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

def get_resource_method(resource):
    resource_method = ResourceMethod(resource["method"])
    resource_client = get_resource_client()

    if resource_method == ResourceMethod.CRASH_RATE:
        return resource_client.vitals().crashrate()
    elif resource_method == ResourceMethod.SLOW_START_RATE:
        return resource_client.vitals().slowstartrate()
    elif resource_method == ResourceMethod.ANR_RATE:
        return resource_client.vitals().anrrate()
    elif resource_method == ResourceMethod.ERROR_COUNT:
        return resource_client.vitals().errors().counts()
    elif resource_method == ResourceMethod.ERROR_ISSUES:
        return resource_client.vitals().errors().issues()
    elif resource_method == ResourceMethod.ERROR_REPORTS:
        return resource_client.vitals().errors().reports()
    
def get_freshness_date(resource):
    if resource["type"] == "METRIC":
        dispatch_resource = get_resource_method(resource)
        data_response = dispatch_resource.get(
                                name=f'apps/{BUNDLE_APP}/{resource["method"]}'
                                ).execute()
        
        freshness_info = next(
                            filter(
                                lambda item:
                                    item["aggregationPeriod"] == AGGREGATION_DEFAULT,
                                    data_response["freshnessInfo"]["freshnesses"]
                            )
                        )
        
        freshness_date_year = freshness_info["latestEndTime"]["year"]
        freshness_date_month = freshness_info["latestEndTime"]["month"]
        freshness_date_day = freshness_info["latestEndTime"]["day"]

        return datetime.strptime(f'{freshness_date_year}-{freshness_date_month}-{freshness_date_day}', "%Y-%m-%d").date()
    else:
        #just a unnecessary return
        return datetime.today().date()

def get_data_from_resource(resource,page_token,start_date, end_date):
    
    dispatch_resource = get_resource_method(resource)
    
    resource_type = resource["type"]
    resource_response = { }
    
    if resource_type == "METRIC":
        body = get_metric_body(resource,page_token,start_date, end_date)
        resource_response = dispatch_resource.query(
                                name=f'apps/{BUNDLE_APP}/{resource["method"]}', 
                                body=body).execute()
    
    elif resource_type == "REPORT":

        page_size = REPORT_PAGE_SIZE
    
        #info_date_now = datetime.utcnow()
        
        resource_response = dispatch_resource.search(
                            parent=f'apps/{BUNDLE_APP}',
                            pageToken=page_token,
                            pageSize=page_size
                            #filter="errorIssueType = CRASH AND versionCode = 123",
                            #interval_startTime_year=info_date_now.year,
                            #interval_startTime_month=info_date_now.month,
                            #interval_startTime_day=info_date_now.day,
                            #interval_startTime_hours=info_date_now.hour - 1,
                            #interval_endTime_year=info_date_now.year,
                            #interval_endTime_month=info_date_now.month,
                            #interval_endTime_day=info_date_now.day,
                            #interval_endTime_hours=info_date_now.hour
                            ).execute()
      
    return resource_response

def main():
    resources = read_json_file(RESOURCES_FILE)
  
    for resource in resources:
        
        resource_method = resource['method']
        resource_type = resource["type"]
        
        start_date = datetime.strptime(START_TIME, "%Y-%m-%d").date()
        end_date = get_freshness_date(resource)
        
        generate_log(log_type.PROCESS_INFO,
                     f'Resource {resource["method"]} will run between the periods of {start_date} and {end_date}')
        
        should_repeat = True
        page_token = ''
        full_data_response = []
        page = 0

        while should_repeat:
            try:
                data_response = get_data_from_resource(resource,page_token,start_date, end_date)
                generate_log(log_type.DATA_READ,f"Retrieved page {page} of the query {resource_method}")
        
                output_key = resource["outputKey"]
                
                if data_response.get(output_key) is not None:
                    full_data_response.extend(data_response[output_key])
                
                if "nextPageToken" in data_response:
                    page_token = data_response["nextPageToken"]
                else:
                    should_repeat = False
                
                page += 1
                print(f'{len(full_data_response)} records read so far..')
                
            except Exception as ex:
                generate_log(log_type.FILE_NOT_CREATED,f"It was not possible to finish obtaining resource {resource_method} while searching for records on page {page}. Error: {ex}")

                      
        directory_to_write_file = f'{RAW_FOLDER}/{BUNDLE_APP}/{resource_type}'
        write_json_file(directory_to_write_file,
                        resource["fileName"],
                        full_data_response)
    
    generate_log(log_type.PROCESS_FINISHED,f"Extract process finished!")
    
main()