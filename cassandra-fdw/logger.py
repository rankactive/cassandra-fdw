from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO

ERROR = ERROR
WARNING = WARNING
DEBUG = DEBUG
INFO = INFO

def log(message, level=INFO, hint=None, detail=None):
    log_to_postgres(message, level, hint, detail)
    