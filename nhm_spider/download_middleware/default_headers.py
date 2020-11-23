from nhm_spider.download_middleware.base import DownloadMiddleware


class DefaultRequestHeadersDownloadMiddleware(DownloadMiddleware):
    def __init__(self):
        self.default_headers = None

    def open_spider(self, spider):
        self.default_headers = spider.settings.get("DEFAULT_REQUEST_HEADER", {})

    def process_request(self, request, spider):
        if request.headers is None:
            request.headers = self.default_headers
        return None
