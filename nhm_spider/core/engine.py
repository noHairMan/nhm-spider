import asyncio
import time

from nhm_spider.common.log import get_logger
from nhm_spider.common.time_counter import time_limit
from nhm_spider.core.downloader import Downloader
from nhm_spider.core.scheduler import Scheduler
from nhm_spider.exceptions import SettingsError


class Engine:
    """
    todo: deprecated
    """
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    # 显示方法执行的时间
    @time_limit(display=True)
    def run(self, spider_class):
        spider = spider_class.from_crawler()

        downloader = Downloader(spider)
        scheduler = Scheduler(spider)

        run_forever = spider.settings.get_bool("RUN_FOREVER")
        if not isinstance(run_forever, bool):
            raise SettingsError(f"Settings param `RUN_FOREVER` must be boolean., got type {type(run_forever)}.")
        if run_forever is True:
            run_loop_interval = spider.settings.get_int("RUN_LOOP_INTERVAL")
            while run_forever:
                asyncio.run(scheduler.crawl(spider, downloader))
                time.sleep(run_loop_interval)
        else:
            asyncio.run(scheduler.crawl(spider, downloader))

    def __del__(self):
        self.logger.info("Engine quit.")
