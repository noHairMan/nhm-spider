from abc import ABC, abstractmethod

from nhm_spider.http.request import Request
from nhm_spider.common.log import get_logger
from nhm_spider.utils.project import get_default_settings
from nhm_spider.settings.settings_manager import SettingsManager


class BaseSpider(ABC):
    name: str
    start_urls: list
    settings: dict
    custom_settings: dict

    @abstractmethod
    def start_request(self):
        """
        启动爬虫任务的方法，需添加启动任务到此处
        """

    @abstractmethod
    def parse(self):
        """
        start_urls里的方法的回调，处理方法。
        """

    @classmethod
    @abstractmethod
    def from_crawler(cls, crawler=None, *args, **kwargs):
        """
        创建实例的类方法
        """


class Spider(BaseSpider):
    name = "Spider"
    start_urls = []
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"{self.__class__.__name__} start.")

    @classmethod
    def from_crawler(cls, crawler=None, *args, **kwargs):
        # todo: crawler is None
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider._set_spider(crawler)
        return spider

    def _set_crawler(self, crawler): ...

    def _set_spider(self, crawler):
        self.crawler = crawler
        # 获取 default_settings
        default_settings = get_default_settings()
        self.settings = SettingsManager(default_settings) | self.custom_settings
        self.DEBUG = self.settings.get_bool("DEBUG")

    async def custom_init(self): ...

    async def custom_close(self): ...

    async def custom_success_close(self): ...

    def start_request(self):
        for url in self.start_urls:
            request = Request(url, callback=self.parse)
            yield request

    def parse(self, response): ...

    def __del__(self):
        self.logger.info(f"{self.__class__.__name__} closed.")
