import asyncio
from typing import Optional
from weakref import WeakValueDictionary, finalize

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector

from nhmspider.core.interface import DownloaderABC, SpiderABC
from nhmspider.http import Request, Response


class AiohttpDownloader(DownloaderABC):
    def __init__(self, spider: SpiderABC):
        super().__init__(spider=spider)
        self.sessions: dict[Optional[str], aiohttp.ClientSession] = {}

    async def get_session(self, request: Optional[Request] = None, new: bool = False) -> aiohttp.ClientSession:
        proxy = request.proxy if request and request.proxy else None
        if proxy not in self.sessions or new:
            connector = ProxyConnector.from_url(request.proxy) if proxy else None
            session = aiohttp.ClientSession(
                connector=connector,
                headers=self.headers,
                timeout=ClientTimeout(self.timeout),
            )
            self.sessions[proxy] = session
        return self.sessions[proxy]

    async def remove_session(self, request: Optional[Request]):
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
            if self.use_session is False:
                session = await self.get_session(request, new=True)
                try:
                    response = await self.send(session, request)
                except:
                    raise
                finally:
                    await self.remove_session(request)
            else:
                session = await self.get_session(request)
                response = await self.send(session, request)
            if response is None:
                return None
            text = await response.text()
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
