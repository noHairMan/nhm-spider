"""
    爬虫基类

    @Time : 2022/4/19 15:09
    @Author : noHairMan
    @File : base.py
    @Project : nhm-spider
"""
from nhm_spider.common.log import get_logger
from nhm_spider.http.request import Request
from nhm_spider.settings.settings_manager import SettingsManager
from nhm_spider.spider.interface import SpiderAbc
from nhm_spider.utils.project import get_default_settings


class Spider(SpiderAbc):
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
