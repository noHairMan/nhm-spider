import aiohttp
from aiohttp import ClientTimeout

from nhm_spider.HTTP.response import Response
from nhm_spider.common.log import get_logger


class Downloader:
    def __init__(self, spider):
        self.logger = get_logger(self.__class__.__name__)
        self.session = None
        self.spider = spider

    async def init(self):
        async def on_request_start(session, trace_config_ctx, params):
            # print("Starting request")
            pass

        async def on_request_end(session, trace_config_ctx, params):
            # print("Ending request")
            pass

        # todo: 应使用spider.settings获取配置
        settings = self.spider.settings
        headers = settings.get("DEFAULT_REQUEST_HEADER", {})
        request_timeout = settings.get("REQUEST_TIMEOUT", 180)
        timeout = ClientTimeout(total=request_timeout)

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)

        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout, trace_configs=[trace_config])

    async def send_request(self, request):
        try:
            if request.method.lower() == "get":
                response = await self.session.get(request.url, headers=request.headers, cookies=request.cookies)
            elif request.method.lower() == "post":
                response = await self.session.post(request.url, data=request.form, headers=request.headers,
                                                   cookies=request.cookies)
            else:
                self.logger.error("传入不支持的方法。")
                return
            text = await response.text()  # TimeoutError
        except Exception as exception:
            return exception
        my_response = Response(request.url, request, text, response, response.status, response.headers)
        return my_response
