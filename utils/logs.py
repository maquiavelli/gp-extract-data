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
    DATA_READ = f"{colors.OKCYAN} {datetime.now()} - "
    FILE_CREATED = f"{colors.OKGREEN} {datetime.now()} - "
    FILE_READ = f"{colors.OKGREEN} {datetime.now()} - "
    PROCESS_FINISHED = f"{colors.ENDC} {datetime.now()} - "


def generate_log(log_type,content):
    print(log_type + content)
    