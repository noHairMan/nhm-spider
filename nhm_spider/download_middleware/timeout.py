from asyncio.exceptions import TimeoutError
from traceback import print_exc

from nhm_spider.common.log import get_logger
from nhm_spider.download_middleware.base import DownloadMiddleware


class TimeoutDownloadMiddleware(DownloadMiddleware):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        # 最大重试次数
        self.max_retry_times = 3

    def process_exception(self, request, exception, spider):
        if isinstance(exception, TimeoutError):
            request.dont_filter = True
            retry_times = request.meta.get("exception_retry_times", 0)
            if retry_times < self.max_retry_times:
                self.logger.info(f"{request} exception: TimeoutError, retry {retry_times + 1} time...")
                request.meta["exception_retry_times"] = retry_times + 1
                return request
            else:
                self.logger.warning(f"{request} exception error, retry {self.max_retry_times} times error.")
                return None
        else:
            # 打印异常
            print_exc()
        return exception
