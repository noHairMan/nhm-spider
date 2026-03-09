from typing import Literal, Optional

from playwright.async_api import Browser, async_playwright

from nhm_spider import Request, Response
from nhm_spider.core.downloader.base import BaseDownloader


class PlaywrightDownloader(BaseDownloader):
    browser_type: Literal["chromium", "firefox", "webkit"]

    async def open_downloader(self):
        await super().open_downloader()
        self.browser_type = self.spider.settings.get_string("BROWSER_TYPE", "chromium")

    async def send_request(self, request: Request) -> Optional[Response | Exception]:
        if request.method.lower() != "get":
            return None

        playwright_kwargs = request.meta.get("playwright", {})
        screenshot = playwright_kwargs.get("screenshot")
        try:
            async with async_playwright() as playwright:
                launch_kwargs = {}
                if request.proxy:
                    launch_kwargs["proxy"] = {"server": request.proxy}
                async with await getattr(playwright, self.browser_type).launch(**launch_kwargs) as browser:
                    browser: Browser
                    page = await browser.new_page()
                    await page.goto(request.url)
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
                        await page.screenshot()
                    response.meta["screenshot"] = screenshot
        except Exception as exception:
            return exception
        return response
