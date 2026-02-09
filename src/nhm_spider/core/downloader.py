from typing import Optional

import aiohttp
from aiohttp import ClientTimeout
from aiohttp_socks import ProxyConnector

from nhm_spider.common.log import get_logger
from nhm_spider.http.response import Response


class Downloader:
    def __init__(self, spider):
        self.logger = get_logger(self.__class__.__name__)
        self.spider = spider
        self.__headers = None
        self.__timeout = None
        self.__clear_cookie = None
        self.__use_session = None
        self.__opened = False
        self.__sessions = {}

    async def open_downloader(self):
        self.__headers = self.spider.settings.get_dict("DEFAULT_REQUEST_HEADER")
        request_timeout = self.spider.settings.get_int("REQUEST_TIMEOUT", 180)
        self.__timeout = ClientTimeout(total=request_timeout)
        self.__clear_cookie = self.spider.settings.get_bool("CLEAR_COOKIE", False)
        self.__use_session = self.spider.settings.get_bool("USE_SESSION", True)

        self.__opened = True

    def close_downloader(self):
        self.__opened = False

    @property
    def is_opened(self):
        return self.__opened

    def get_session(self, request=None):
        proxy = request.proxy if request and request.proxy else None
        if (session := self.__sessions.get(proxy)) is not None:
            return session

        async def on_request_start(session, trace_config_ctx, params):
            # print("Starting request")
            pass

        async def on_request_end(session, trace_config_ctx, params):
            # print("Ending request")
            pass

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)

        connector = ProxyConnector.from_url(request.proxy) if proxy else None
        self.__sessions[proxy] = aiohttp.ClientSession(
            connector=connector,
            headers=self.__headers,
            timeout=self.__timeout,
            trace_configs=[trace_config],
        )
        return self.get_session(request)

    def remove_session(self, request):
        proxy = request.proxy if request and request.proxy else None
        self.__sessions.pop(proxy, None)

    async def send_request(self, request) -> Optional[Response | Exception]:
        try:
            # 是否每次创建新session请求
            if self.__use_session is False:
                session = self.get_session(request)
                response = await self.send(session, request)
                self.remove_session(request)
            else:
                session = self.get_session(request)
                # 每次请求前清除session缓存的cookies 为response set-cookie中自动缓存的
                if self.__clear_cookie is True:
                    session.cookie_jar.clear()
                response = await self.send(session, request)
            if response is None:
                return
            # 获取完text之后，会自动关闭response。
            text = await response.text()  # TimeoutError
        except Exception as exception:
            return exception
        my_response = Response(
            request.url,
            request,
            text,
            response,
            response.status,
            response.headers,
        )
        return my_response

    async def send(self, session: aiohttp.ClientSession, request):
        """处理不同method的请求参数"""
        if request.method.lower() == "get":
            response = await session.get(
                request.url,
                data=request.body,
                headers=request.headers,
                cookies=request.cookies,
            )
        elif request.method.lower() == "post":
            response = await session.post(
                request.url,
                data=request.form,
                headers=request.headers,
                cookies=request.cookies,
            )
        else:
            self.logger.error("传入不支持的方法。")
            response = None
        return response
