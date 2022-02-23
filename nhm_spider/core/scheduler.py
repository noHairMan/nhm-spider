import asyncio
from inspect import isawaitable, iscoroutine
from traceback import format_exc
from types import GeneratorType, AsyncGeneratorType

from nhm_spider.common.log import get_logger
from nhm_spider.http import Request, Response
from nhm_spider.item import Item
from nhm_spider.utils.pqueue import SpiderPriorityQueue
from nhm_spider.utils.request import request_fingerprint
from nhm_spider.utils.signal import SignalManager


class Scheduler:
    def __init__(self, spider):
        self.logger = get_logger(self.__class__.__name__)
        self.request_queue = None
        self.signal_manager = None
        # 请求去重队列
        self.dupe_memory_queue = set()
        self.spider = spider
        self.item_count = 0
        self.request_count = 0
        self.__opened = False

    async def open_scheduler(self):
        self.request_queue = SpiderPriorityQueue()
        self.__opened = True

    def close_scheduler(self):
        self.__opened = False

    @property
    def is_opened(self):
        return self.__opened

    async def next_request(self):
        request = await self.request_queue.get()
        return request

    async def enqueue_request(self, request):
        assert request.callback, "未指定回调方法。"
        await self.request_queue.put(request)

    async def heartbeat(self, heartbeat_interval=60):
        """
        todo: 统计采集的页数，抓取的条数。
              考虑放到extensions中去
        """
        last_item_count = 0
        last_request_count = 0
        while True:
            request_speed = self.request_count - last_request_count
            last_request_count = self.request_count
            item_speed = self.item_count - last_item_count
            last_item_count = self.item_count
            self.logger.info(f"Crawled {last_request_count} pages (at {request_speed} pages/min), "
                             f"scraped {last_item_count} items (at {item_speed} items/min), "
                             f"queue size {self.request_queue.qsize()}.")

            await asyncio.sleep(heartbeat_interval)
