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
