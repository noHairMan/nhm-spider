import aiohttp
from aiohttp import ClientTimeout

from nhm_spider.http.response import Response
from nhm_spider.common.log import get_logger


class Downloader:
    def __init__(self, spider):
        self.logger = get_logger(self.__class__.__name__)
        self.session = None
        self.spider = spider
        self.__headers = None
        self.__timeout = None
        self.__clear_cookie = None
        self.__use_session = None

    async def init(self):
        async def on_request_start(session, trace_config_ctx, params):
            # print("Starting request")
            pass

        async def on_request_end(session, trace_config_ctx, params):
            # print("Ending request")
            pass

        self.__headers = self.spider.settings.get("DEFAULT_REQUEST_HEADER", {})
        request_timeout = self.spider.settings.get("REQUEST_TIMEOUT", 180)
        self.__timeout = ClientTimeout(total=request_timeout)
        self.__clear_cookie = self.spider.settings.get("CLEAR_COOKIE", False)
        self.__use_session = self.spider.settings.get("USE_SESSION", True)

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)

        self.session = aiohttp.ClientSession(headers=self.__headers, timeout=self.__timeout,
                                             trace_configs=[trace_config])

    async def send_request(self, request):
        try:
            # 每次请求前清除session缓存的cookies 为response set-cookie中自动缓存的
            if self.__clear_cookie is True:
                self.session.cookie_jar.clear()
            # 是否每次创建新session请求
            if self.__use_session is False:
                session = aiohttp.ClientSession(headers=self.__headers, timeout=self.__timeout)
                response = await self.send(session, request)
                await session.close()
            else:
                response = await self.send(self.session, request)
            if response is None:
                return
            # 获取完text之后，会自动关闭response。
            text = await response.text()  # TimeoutError
        except Exception as exception:
            return exception
        my_response = Response(request.url, request, text, response, response.status, response.headers)
        return my_response

    async def send(self, session, request):
        """ 处理不同method的请求参数 """
        if request.method.lower() == "get":
            response = await session.get(request.url, data=request.body, headers=request.headers,
                                         cookies=request.cookies, proxy=request.proxy)
        elif request.method.lower() == "post":
            response = await session.post(request.url, data=request.form, headers=request.headers,
                                          cookies=request.cookies, proxy=request.proxy)
        else:
            self.logger.error("传入不支持的方法。")
            response = None
        return response
