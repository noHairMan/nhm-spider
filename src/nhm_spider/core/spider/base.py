from nhm_spider.core.interface import CrawlerABC, SpiderABC
from nhm_spider.http.request import Request
from nhm_spider.settings.settings_manager import SettingsManager
from nhm_spider.utils.log import get_logger
from nhm_spider.utils.project import get_default_settings


class Spider(SpiderABC):
    name = "Spider"
    start_urls = []
    custom_settings = {}

    _settings: SettingsManager
    crawler: CrawlerABC

    def __init__(self, *args, **kwargs):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"{self.__class__.__name__} start.")

    @property
    def settings(self):
        return self._settings

    @classmethod
    def from_crawler(cls, crawler: CrawlerABC, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider._set_spider(crawler)
        return spider

    def _set_crawler(self, crawler: CrawlerABC): ...

    def _set_spider(self, crawler):
        self.crawler = crawler
        # 获取 default_settings
        default_settings = get_default_settings()
        self._settings = SettingsManager(default_settings) | self.custom_settings
        self.DEBUG = self.settings.get_boolean("DEBUG")

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
