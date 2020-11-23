class DownloaderMiddlewareManager:
    """
    todo: 下载中间件管理
    """
    def download(self, download_func, request, spider):
        pass


class DownloadMiddleware:
    def open_spider(self, spider):
        pass

    def process_request(self, request, spider):
        pass

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        return exception

    def close_spider(self, spider):
        pass


class AsyncDownloadMiddleware:
    async def open_spider(self, spider):
        pass

    async def process_request(self, request, spider):
        pass

    async def process_response(self, request, response, spider):
        return response

    async def process_exception(self, request, exception, spider):
        return exception

    async def close_spider(self, spider):
        pass
