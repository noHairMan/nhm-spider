from enum import Enum


class ExceptionEnum(Enum):
    """
    异常的错误信息提示
    """
    TYPE_ERROR = "未知的对象类型"


class NhmException(Exception):
    pass


class SettingsError(NhmException):
    pass


class SettingsTypeError(SettingsError):
    """
    获取设置时转换类型错误
    """
    def __init__(self, key, value, _type, ):
        self.__key = key
        self.__value = value
        self.__type = _type

    def __str__(self):
        return f"Settings keyword argument [{self.__key}] need type {self.__type} but value is [{self.__value}]."


class NoCrawlerError(NhmException):
    """
    没有添加的爬虫任务
    """
