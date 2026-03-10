from abc import ABC, abstractmethod
from typing import Optional

from nhm_spider import Spider
from nhm_spider.http import Request, Response
from nhm_spider.utils.log import get_logger


class BaseDownloader(ABC):
    def __init__(self, spider: Spider):
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
