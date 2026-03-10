from typing import Optional

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector

from nhmspider.core.interface import DownloaderABC, SpiderABC
from nhmspider.http import Request, Response


class AiohttpDownloader(DownloaderABC):
    def __init__(self, spider: SpiderABC):
        super().__init__(spider=spider)
        self.sessions = {}

    async def get_session(self, request=None):
        proxy = request.proxy if request and request.proxy else None
        if (session := self.sessions.get(proxy)) is not None:
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
        self.sessions[proxy] = aiohttp.ClientSession(
            connector=connector,
            headers=self.headers,
            timeout=ClientTimeout(self.timeout),
            trace_configs=[trace_config],
        )
        return await self.get_session(request)

    async def remove_session(self, request):
        proxy = request.proxy if request and request.proxy else None
        session = self.sessions.pop(proxy, None)
        if session is not None:
            await session.close()

    async def close_downloader(self):
        await super().close_downloader()
        for key in tuple(self.sessions.keys()):
            session = self.sessions.pop(key)
            if isinstance(session, ClientSession):
                await session.close()

    async def send_request(self, request) -> Optional[Response | Exception]:
        try:
            # 是否每次创建新session请求
            if self.use_session is False:
                session = await self.get_session(request)
                response = await self.send(session, request)
                await self.remove_session(request)
            else:
                session = await self.get_session(request)
                # 每次请求前清除session缓存的cookies 为response set-cookie中自动缓存的
                if self.clear_cookie is True:
                    session.cookie_jar.clear()
                response = await self.send(session, request)
            if response is None:
                return
            # 获取完text之后，会自动关闭response。
            text = await response.text()  # TimeoutError
        except Exception as exception:
            return exception
        return Response(
            request.url,
            request,
            text,
            response,
            response.status,
            response.headers,
        )

    async def send(self, session: aiohttp.ClientSession, request: Request) -> Optional[Response]:
        """处理不同method的请求参数"""
        timeout = aiohttp.ClientTimeout(total=(request.timeout or self.timeout))
        if request.method.lower() == "get":
            response = await session.get(
                request.url,
                data=request.body,
                headers=request.headers,
                cookies=request.cookies,
                timeout=timeout,
            )
        elif request.method.lower() == "post":
            response = await session.post(
                request.url,
                data=request.form,
                headers=request.headers,
                cookies=request.cookies,
                timeout=timeout,
            )
        else:
            response = None
        return response
