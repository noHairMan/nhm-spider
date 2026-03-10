from typing import Optional
from weakref import WeakValueDictionary

from playwright.async_api import Browser, Playwright, PlaywrightContextManager, async_playwright

from nhmspider.core.interface import DownloaderABC, SpiderABC
from nhmspider.http import Request, Response
from nhmspider.utils.hash import hash_dictionary


class PlaywrightDownloader(DownloaderABC):
    __playwright_context_manager: PlaywrightContextManager
    __playwright: Playwright

    def __init__(self, spider: SpiderABC):
        super().__init__(spider)
        # Literal ["chromium", "firefox", "webkit"]
        self.browser_type = self.spider.settings.get_string("BROWSER_TYPE", "chromium")
        self.browsers = WeakValueDictionary()

    async def open_downloader(self):
        self.__playwright_context_manager = async_playwright()
        self.__playwright = await self.__playwright_context_manager.__aenter__()
        return await super().open_downloader()

    async def close_downloader(self):
        await self.__playwright_context_manager.__aexit__()
        return await super().close_downloader()

    async def get_browser(self, launch_kwargs: dict) -> Browser:
        hash = hash_dictionary(launch_kwargs)
        if hash not in self.browsers:
            self.browsers[hash] = await getattr(self.__playwright, self.browser_type).launch(**launch_kwargs)
        return self.browsers[hash]

    async def send_request(self, request: Request) -> Optional[Response | Exception]:
        if request.method.lower() != "get":
            return None

        playwright_kwargs = request.meta.get("playwright", {})
        screenshot = playwright_kwargs.get("screenshot")

        launch_kwargs = {}
        if request.proxy:
            launch_kwargs["proxy"] = {"server": request.proxy}
        try:
            browser = await self.get_browser(launch_kwargs)
            page = await browser.new_page()
            await page.goto(request.url, timeout=(request.timeout or self.timeout) * 1000)
            text = await page.content()
            response = Response(
                url=page.url,
                request=request,
                text=text,
                response=None,
                status=200,
                headers={},
            )
            if screenshot is not None:
                response.meta["screenshot"] = await page.screenshot()
        except Exception as exception:
            return exception
        return response
