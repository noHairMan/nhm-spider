from abc import ABC, abstractmethod
from logging import Logger
from typing import Any, AsyncGenerator, Generator, Optional

from nhm_spider.http import Request, Response
from nhm_spider.item.base import Item
from nhm_spider.utils.log import get_logger
from nhm_spider.utils.time_counter import time_limit


class CrawlerABC(ABC):
    spider: SpiderABC
    logger: Logger

    @time_limit(display=True)
    async def run(self):
        await self.crawl()

    @abstractmethod
    async def crawl(self):
        raise NotImplementedError


TypeSpiderOut = Generator[Request | Item, None, None] | AsyncGenerator[Request | Item, None]


class SpiderABC(ABC):
    # 爬虫类的名称，spider类的logger会使用此名称创建
    name: str
    # 启动爬虫的初始链接，该数组中的链接会以get方式发送请求。
    start_urls: list[str]
    # 当前spider的专用配置
    custom_settings: dict[str, Any]

    @abstractmethod
    def start_request(self) -> TypeSpiderOut: ...

    @abstractmethod
    def parse(self, response: Response) -> TypeSpiderOut: ...

    @classmethod
    @abstractmethod
    def from_crawler(cls, crawler: CrawlerABC, *args, **kwargs): ...


class DownloaderABC(ABC):
    def __init__(self, spider: SpiderABC):
        self.logger = get_logger(self.__class__.__name__)
        self.spider = spider
        self.headers = None
        self.timeout = self.spider.settings.get_float("REQUEST_TIMEOUT")
        self.__opened: bool = False

        self.clear_cookie = self.spider.settings.get_boolean("CLEAR_COOKIE")
        self.use_session = self.spider.settings.get_boolean("USE_SESSION")

    async def open_downloader(self):
        self.headers = self.spider.settings.get_dict("DEFAULT_REQUEST_HEADER")
        self.__opened = True

    async def close_downloader(self):
        self.__opened = False

    @property
    def is_opened(self) -> bool:
        return self.__opened

    @abstractmethod
    async def send_request(self, request: Request) -> Optional[Response | Exception]:
        raise NotImplementedError
