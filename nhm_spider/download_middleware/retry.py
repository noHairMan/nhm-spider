from nhm_spider.HTTP.response import Response
from nhm_spider.common.log import get_logger
from nhm_spider.download_middleware.base import DownloadMiddleware


class RetryDownloadMiddleware(DownloadMiddleware):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        # 最大重试次数
        self.max_retry_times = 3
        self.ignore_http_error = None

    def open_spider(self, spider):
        self.ignore_http_error = spider.settings.get("IGNORE_HTTP_ERROR", [])

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response

        if isinstance(response, Response) and response.status != 200:
            if response.status in self.ignore_http_error:
                # 处理忽略的错误状态码
                return response

            if request.dont_filter is not True:
                request.dont_filter = True
            retry_times = request.meta.get("retry_times", 0)
            if retry_times < self.max_retry_times:
                self.logger.info(f"({response.status}) {response} status error, retry {retry_times + 1} time...")
                request.meta["retry_times"] = retry_times + 1
                return request
            else:
                self.logger.warning(f"{response} retry max {self.max_retry_times} times error。")
                return None
        return response
