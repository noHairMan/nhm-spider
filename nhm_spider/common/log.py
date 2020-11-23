from logging import Formatter, getLogger, StreamHandler, INFO, Logger, FileHandler, CRITICAL


__instance = {}


def get_logger(name="common", log_level=None, formatter=None):
    """
    单角色模式
    """
    if name in __instance:
        return __instance[name]
    formatter_option = formatter or f'%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    logger_formatter = Formatter(formatter_option)
    handler = StreamHandler()
    handler.setFormatter(logger_formatter)
    handler.setLevel(log_level or INFO)
    _logger = getLogger(name)
    _logger.addHandler(handler)
    _logger.setLevel(INFO)
    __instance[name] = _logger
    return _logger


def add_format(_logger: Logger, formatter):
    logger_formatter = Formatter(formatter)
    for handler in _logger.handlers:
        handler.setFormatter(logger_formatter)


def add_file_handler(_logger: Logger, file_name, log_level=None):
    """
    使用文件存储日志。
    """
    handler = FileHandler(file_name)
    for ori_handler in _logger.handlers:
        if isinstance(ori_handler, StreamHandler):
            handler.setFormatter(ori_handler.formatter)
    handler.setLevel(log_level or INFO)
    _logger.addHandler(handler)


def get_file_logger(name=None, file_name=None, stream_log_level=None, file_log_level=None):
    assert file_name, "日志文件名不能为空。"
    _logger = get_logger(name, stream_log_level)
    add_file_handler(_logger, file_name, file_log_level)
    return _logger


def test():
    file_name = "/disk1t/logs/test.log"
    logger = get_file_logger(file_name=file_name, stream_log_level=CRITICAL)
    formatter_option = f'%(asctime)s [%(name)s] %(levelname)s: %(message)s  测试!'
    add_format(logger, formatter_option)
    logger.info("123")


if __name__ == '__main__':
    test()

