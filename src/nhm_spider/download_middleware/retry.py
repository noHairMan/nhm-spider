from nhm_spider.download_middleware.base import DownloadMiddleware
from nhm_spider.http.response import Response
from nhm_spider.common.log import get_logger


class RetryDownloadMiddleware(DownloadMiddleware):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        # 最大重试次数
        self.max_retry_times = 3
        self.ignore_http_error = None

    def open_spider(self, spider):
        self.ignore_http_error = spider.settings.get_dict("IGNORE_HTTP_ERROR")

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response

        if isinstance(response, Response) and response.status != 200:
            if response.status in self.ignore_http_error:
                # 处理忽略的错误状态码
                return response

            return self._retry(request, response)
        return response

    def process_exception(self, request, exception, spider):
        # todo: 待设置指定异常重试
        # if (
        #     isinstance(exception, self.EXCEPTIONS_TO_RETRY)
        #     and not request.meta.get('dont_retry', False)
        # ):
        #     return self._retry(request, exception)
        return self._retry(request, exception.__class__.__name__)

    def _retry(self, request, reason):
        # todo: 待验证重试是否正确
        if request.dont_filter is not True:
            request.dont_filter = True
        retry_times = request.meta.get("retry_times", 1)
        if retry_times < self.max_retry_times:
            self.logger.info(f"{reason}, retry {retry_times} time...")
            request.meta["retry_times"] = retry_times + 1
            return request
        else:
            self.logger.warning(f"{reason} retry max {self.max_retry_times} times error。")
            return None
