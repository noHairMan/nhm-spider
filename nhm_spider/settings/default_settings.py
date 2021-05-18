from nhm_spider.download_middleware.default_headers import DefaultRequestHeadersDownloadMiddleware
from nhm_spider.download_middleware.retry import RetryDownloadMiddleware
from nhm_spider.download_middleware.timeout import TimeoutDownloadMiddleware

# 请求队列的最大值，控制此值可控制内存使用
REQUEST_QUEUE_MAX_SIZE = 1000
# 是否使用session
USE_SESSION = True
# 是否清理session的cookie
# USE_SESSION = False时不生效
CLEAR_COOKIE = False
# 并发数量，即启动的任务数量
CONCURRENT_REQUESTS = 8
# 默认请求头
DEFAULT_REQUEST_HEADER = {
    'User-Agent': 'nhm-spider/1.23'
}
# 是否开启调试日志
DEBUG = True
# 日志输出等级
DEBUG_LEVEL = "INFO"
# 默认请求超时时间，30秒
REQUEST_TIMEOUT = 30
# 默认开启的管道
ENABLED_PIPELINE = [
    # TmPipeline
    # TmQueryPipeline
]
# 默认开启的中间件
ENABLED_DOWNLOAD_MIDDLEWARE = [
    DefaultRequestHeadersDownloadMiddleware,
    RetryDownloadMiddleware,
    TimeoutDownloadMiddleware,
]
# 忽略的状态码错误
IGNORE_HTTP_ERROR = []
