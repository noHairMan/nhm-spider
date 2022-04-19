from logging import Formatter, StreamHandler, INFO, Logger
from typing import Union


class LoggerManager:
    __instance = {}

    @staticmethod
    def hash(name, log_level):
        return hash((name, log_level))

    @classmethod
    def get_logger(cls, name: str = "default", log_level=None):
        """
        单例
        """
        hash_code = cls.hash(name, log_level)
        if hash_code in cls.__instance:
            return cls.__instance[hash_code]
        formatter_option = f'%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        logger_formatter = Formatter(formatter_option)
        handler = StreamHandler()
        handler.setFormatter(logger_formatter)
        handler.setLevel(log_level or INFO)
        _logger = Logger(name)
        _logger.addHandler(handler)
        _logger.setLevel(log_level or INFO)
        cls.__instance[hash_code] = _logger
        return _logger

    @classmethod
    def set_log_level(cls, logger: Logger, level: Union[str, int]):
        [handler.setLevel(level) for handler in logger.handlers]
        logger.setLevel(level)


get_logger = LoggerManager.get_logger
