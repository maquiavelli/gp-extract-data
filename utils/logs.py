from datetime import datetime

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
class log_type:
    DATA_READ = colors.OKCYAN
    FILE_CREATED = colors.OKGREEN
    FILE_READ = colors.OKCYAN
    PROCESS_FINISHED = colors.ENDC
    FILE_NOT_FOUND = colors.WARNING
    PROCESS_INFO = colors.WARNING
    FILE_NOT_CREATED = colors.FAIL

def generate_log(log_type,content):
    print(f"{log_type} {datetime.now()} - {content}")
    