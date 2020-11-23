import asyncio

from nhm_spider.common.log import get_logger
from nhm_spider.common.time_counter import time_limit
from nhm_spider.core.downloader import Downloader
from nhm_spider.core.scheduler import Scheduler


class Engine:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    # 显示方法执行的时间
    @time_limit(display=True)
    def run(self, spider_class):
        spider = spider_class.from_crawler()
        if not hasattr(spider, "custom_settings"):
            spider.custom_settings = {}

        downloader = Downloader(spider)
        scheduler = Scheduler(spider)

        asyncio.run(scheduler.crawl(spider, downloader))

    def __del__(self):
        self.logger.info("Engine quit.")
