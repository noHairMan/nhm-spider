from nhm_spider.download_middleware.base import DownloadMiddleware
from nhm_spider.http import Headers


class DefaultRequestHeadersDownloadMiddleware(DownloadMiddleware):
    def __init__(self):
        self.default_headers = None

    def open_spider(self, spider):
        self.default_headers = Headers(spider.settings.get_dict("DEFAULT_REQUEST_HEADER"))

    def process_request(self, request, spider):
        # todo: 处理headers大小写为统一规则，否则合并会出现多个相同headers。
        if request.headers is None:
            request.headers = self.default_headers
        else:
            # 合并默认headers和当前request的headers
            request.headers = self.default_headers | request.headers
        return None
