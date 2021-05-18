from nhm_spider.http.request import Request
from nhm_spider.common.log import get_logger


class Spider:
    start_urls = []
    settings = None
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"{self.__class__.__name__} start.")

    @classmethod
    def from_crawler(cls, crawler=None, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider._set_spider()
        return spider

    def _set_crawler(self, crawler):
        pass

    def _set_spider(self):
        # todo: 需合并default_settings文件里的设置
        self.settings = self.custom_settings
        self.DEBUG = self.settings.get("DEBUG", True)

    async def custom_init(self):
        pass

    def start_request(self):
        for url in self.start_urls:
            request = Request(url, callback=self.parse)
            yield request

    def parse(self, response):
        pass

    def __del__(self):
        self.logger.info(f"{self.__class__.__name__} closed.")
