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
        self.browsers: dict[str, Browser] = {}

    async def open_downloader(self):
        self.__playwright_context_manager = async_playwright()
        self.__playwright = await self.__playwright_context_manager.__aenter__()
        return await super().open_downloader()

    async def close_downloader(self):
        for browser in tuple(self.browsers.values()):
            await browser.close()
        self.browsers.clear()
        await self.__playwright_context_manager.__aexit__()
        return await super().close_downloader()

    async def get_browser(self, launch_kwargs: dict) -> Browser:
        key = hash_dictionary(launch_kwargs)
        if key not in self.browsers:
            self.browsers[key] = await getattr(self.__playwright, self.browser_type).launch(**launch_kwargs)
        return self.browsers[key]

    async def send_request(self, request: Request) -> Optional[Response | Exception]:
        if request.method.lower() != "get":
            return None

        playwright_kwargs = request.meta.get("playwright", {})
        save_screenshot = playwright_kwargs.get("screenshot") is True

        launch_kwargs = {}
        if request.proxy:
            launch_kwargs["proxy"] = {"server": request.proxy}
        try:
            browser = await self.get_browser(launch_kwargs)
            async with await browser.new_page() as page:
                await page.goto(request.url, timeout=(request.timeout or self.timeout) * 1000)
                text = await page.content()
                screenshot_bytes = None
                if save_screenshot is not None:
                    screenshot_bytes = await page.screenshot()
        except Exception as exception:
            return exception

        response = Response(
            url=page.url,
            request=request,
            text=text,
            response=None,
            status=200,
            headers={},
        )
        response.meta["screenshot"] = screenshot_bytes
        return response
