import time
from datetime import datetime


def timestamp_to_date(timestamp, format_string="%Y-%m-%d %H:%M:%S"):
    """
    时间戳转为日期
    """
    return time.strftime(format_string, time.localtime(timestamp))


def date_to_timestamp(date_string, format_string="%Y-%m-%d %H:%M:%S"):
    """
    日期转换为时间戳
    """
    return int(time.mktime(time.strptime(date_string, format_string)))


def get_date(format_string='%Y-%m-%d'):
    return datetime.now().strftime(format_string)
