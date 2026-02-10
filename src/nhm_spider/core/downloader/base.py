from abc import ABC, abstractmethod
from typing import Optional

from aiohttp import ClientTimeout

from nhm_spider import Spider
from nhm_spider.common.log import get_logger
from nhm_spider.http import Request, Response


class BaseDownloader(ABC):
    def __init__(self, spider: Spider):
        self.logger = get_logger(self.__class__.__name__)
        self.spider = spider
        self.headers = None
        self.timeout = self.spider.settings.get_int("REQUEST_TIMEOUT", 180)
        self.__opened: bool = False

        self.clear_cookie = self.spider.settings.get_bool("CLEAR_COOKIE", False)
        self.use_session = self.spider.settings.get_bool("USE_SESSION", True)

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
        raise NotImplementedError()
