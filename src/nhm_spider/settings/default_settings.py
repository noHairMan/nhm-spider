import logging

VERSION = "3.4.1"
# 是否使用session
USE_SESSION = True
# 是否清理session的cookie，USE_SESSION = False时不生效
CLEAR_COOKIE = False
# 并发数量，即启动的任务数量
CONCURRENT_REQUESTS = 8
# 默认请求头
DEFAULT_REQUEST_HEADER = {"User-Agent": f"nhm-spider/{VERSION}"}
# 是否开启调试日志
DEBUG = True
# 日志输出等级
DEBUG_LEVEL = "INFO"
# 默认请求超时时间，30秒
REQUEST_TIMEOUT = 30
# 默认开启的管道
ENABLED_PIPELINE = []
# 默认开启的中间件
ENABLED_DOWNLOAD_MIDDLEWARE = [
    "nhm_spider.download_middleware.default_headers.DefaultRequestHeadersDownloadMiddleware",
    "nhm_spider.download_middleware.retry.RetryDownloadMiddleware",
    "nhm_spider.download_middleware.timeout.TimeoutDownloadMiddleware",
]
# 忽略的状态码错误
IGNORE_HTTP_ERROR = []
# 允许的成功的请求状态码
SUCCESS_HTTP_CODE = [200]
# 是否循环执行爬虫
RUN_FOREVER = False
# 每次采集完等待间隔开始下一轮
# 默认：1天
RUN_LOOP_INTERVAL = 60 * 60 * 24
# 默认下载器
DEFAULT_DOWNLOADER_CLASS = "nhm_spider.core.downloader.AiohttpDownloader"

LOG_LEVEL = logging.getLevelName(logging.DEBUG)
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(levelname)s] %(asctime)s.%(msecs).3d %(filename)s(%(lineno)s) > %(funcName)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {"format": "[%(levelname)s] %(asctime)s.%(msecs).3d: %(message)s", "datefmt": "%Y-%m-%d %H:%M:%S"},
        "console": {
            "format": "[%(levelname)s] %(asctime)s.%(msecs).3d %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
}
