from nhm_spider.http.request import Request
from nhm_spider.common.log import get_logger
from nhm_spider.utils.project import get_default_settings
from nhm_spider.settings.settings_manager import SettingsManager


class Spider:
    start_urls = []
    settings = None
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

    def _set_crawler(self, crawler):
        # crawler.spider = self
        pass

    def _set_spider(self, crawler):
        self.crawler = crawler
        # 获取 default_settings
        default_settings = get_default_settings()
        self.settings = SettingsManager(default_settings) | self.custom_settings
        self.DEBUG = self.settings.get_bool("DEBUG")

    async def custom_init(self):
        pass

    async def custom_close(self):
        pass

    async def custom_success_close(self):
        pass

    def start_request(self):
        for url in self.start_urls:
            request = Request(url, callback=self.parse)
            yield request

    def parse(self, response):
        pass

    def __del__(self):
        self.logger.info(f"{self.__class__.__name__} closed.")
