import asyncio
from pprint import pformat
from traceback import format_exc
from types import GeneratorType, AsyncGeneratorType
from inspect import isawaitable, iscoroutine

from scrapy.utils.request import request_fingerprint

from nhm_spider.http.request import Request
from nhm_spider.http.response import Response
from nhm_spider.common.log import get_logger
from nhm_spider.item.base import Item
from nhm_spider.utils.pqueue import SpiderPriorityQueue
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
        self.tasks = []

        settings = spider.settings

        # pipeline
        self.concurrent_requests = settings.get("CONCURRENT_REQUESTS", 8)
        enabled_pipeline = settings.get("ENABLED_PIPELINE", [])
        self.enabled_pipeline = [cls() for cls in enabled_pipeline]
        # download middleware
        enabled_download_middleware = settings.get("ENABLED_DOWNLOAD_MIDDLEWARE", [])
        self.enabled_download_middleware = [cls() for cls in enabled_download_middleware]
        # spider middleware
        # enabled_spider_middleware = settings.get("ENABLED_SPIDER_MIDDLEWARE", [])
        # self.enabled_spider_middleware = [cls() for cls in enabled_spider_middleware]

    async def init(self):
        self.request_queue = SpiderPriorityQueue()
        self.signal_manager = SignalManager(self.request_queue)
        self.signal_manager.connect()

    async def next_request(self):
        request = await self.request_queue.get()
        return request

    async def crawl(self, spider, downloader):
        """
        协程主程序
        """
        await self.init()
        await downloader.init()
        await spider.custom_init()

        # init pipeline
        for pipeline in self.enabled_pipeline:
            # 确认是否使用的异步的pipeline
            if not hasattr(pipeline, "open_spider"):
                continue
            pip = pipeline.open_spider(spider)
            if isawaitable(pip):
                await pip

        # init download middleware
        for middleware in self.enabled_download_middleware:
            # 确认是否使用的异步的middleware
            if not hasattr(middleware, "open_spider"):
                continue
            mid = middleware.open_spider(spider)
            if isawaitable(mid):
                await mid

        tasks = self.tasks
        try:
            # 初始化
            results = spider.start_request()
            await self.process_results(results)
            for task_number in range(self.concurrent_requests):
                task = asyncio.create_task(self.process(downloader))
                tasks.append(task)

            # 阻塞并等待所有任务完成
            tasks.append(asyncio.create_task(self.heartbeat()))
            await self.request_queue.join()

        finally:
            # clear pipeline
            for pipeline in self.enabled_pipeline:
                # 确认是否使用的异步的pipeline
                if not hasattr(pipeline, "close_spider"):
                    continue
                pip = pipeline.close_spider(spider)
                if isawaitable(pip):
                    await pip

            # clear download middleware
            for middleware in self.enabled_download_middleware:
                # 确认是否使用的异步的middleware
                if not hasattr(middleware, "close_spider"):
                    continue
                mid = middleware.close_spider(spider)
                if isawaitable(mid):
                    await mid

            await downloader.session.close()
            # 所有task完成后，取消任务，退出程序
            for task in tasks:
                task.cancel()
            # 等待task取消完成
            await asyncio.gather(*tasks, return_exceptions=True)

    async def heartbeat(self, heartbeat_interval=60):
        """
        todo: 统计采集的页数，抓取的条数。
        todo: 考虑放到extensions中去
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

    async def process_results(self, results, response=None):
        if results:
            if isinstance(results, GeneratorType):
                for obj in results:
                    await self.process_result_single(obj, response)
            elif isinstance(results, AsyncGeneratorType):
                async for obj in results:
                    await self.process_result_single(obj, response)
            elif isinstance(results, Request):
                await self.process_result_single(results, response)
            elif iscoroutine(results):
                await results
            else:
                # todo: 考虑如何处理
                self.logger.error(f"丢弃该任务，未处理的处理结果类型：{results}。")

    async def process_result_single(self, obj, response):
        if isinstance(obj, Request):
            # 处理request对象优先级，深度优先
            if obj.priority is None:
                if response is not None:
                    obj.priority = response.request.priority - 1
                else:
                    obj.priority = 0

            fp = request_fingerprint(obj)
            # 根据指纹去重。
            if obj.dont_filter is True or fp not in self.dupe_memory_queue:
                obj.fp = fp
                await self.enqueue_request(obj)
                self.dupe_memory_queue.add(fp)

        elif isinstance(obj, Item):
            if not self.enabled_pipeline and self.spider.DEBUG is True:
                self.logger.info(pformat(obj))
            self.item_count += 1

            for pipeline in self.enabled_pipeline:
                # 确认是否使用的异步的pipeline
                if not hasattr(pipeline, "process_item"):
                    continue
                obj = pipeline.process_item(obj, self.spider)
                if isawaitable(obj):
                    obj = await obj

        else:
            self.logger.warning(f"[yield]尚未处理的类型[{obj.__class__.__name__}]。")

    async def enqueue_request(self, request):
        assert request.callback, "未指定回调方法。"
        await self.request_queue.put(request)

    async def process(self, downloader):
        while True:
            request = await self.next_request()

            response = await self.download_request(request, downloader)
            if not isinstance(response, Response):
                # todo: 待处理非response的情况

                # 失败的请求也要调用task_done，否则无法结束。
                self.request_queue.task_done()
                self.request_count += 1
                continue
            else:
                if self.spider.DEBUG is True:
                    self.logger.info(f"Crawled ({response.status}) {response}.")

            # todo: process_spider_in
            results = request.callback(response)
            # todo: process_spider_out 非此位置
            try:
                await self.process_results(results, response)
            except:
                self.logger.error(format_exc())
            finally:
                self.request_queue.task_done()
                self.request_count += 1

    async def download_request(self, request, downloader):
        # process_request
        for middleware in self.enabled_download_middleware:
            # 确认是否使用的异步的middleware
            if not hasattr(middleware, "process_request"):
                continue
            result = middleware.process_request(request, self.spider)
            if isawaitable(result):
                result = await result

            if result is None:
                pass
            elif isinstance(result, Request):
                return await self.process_results(result)
            elif isinstance(result, Response):
                # 返回response则直接跳过process_request
                request = result
                break
            else:
                self.logger.error(f"未知的对象类型，{request}。")
                raise TypeError("未知的对象类型")

        if isinstance(request, Request):
            response = await downloader.send_request(request)
        elif isinstance(request, Response):
            response = request
        else:
            self.logger.error(f"未知的对象类型，{request}。")
            raise TypeError("未知的对象类型")

        # process_response
        if isinstance(response, Response):
            for middleware in self.enabled_download_middleware:
                # 确认是否使用的异步的middleware
                if not hasattr(middleware, "process_response"):
                    continue
                result = middleware.process_response(request, response, self.spider)
                if isawaitable(result):
                    result = await result

                if result is None:
                    pass
                elif isinstance(result, Request):
                    return await self.process_results(result)
                elif isinstance(result, Response):
                    response = result
                    break
        elif isinstance(response, Exception):
            for middleware in self.enabled_download_middleware:
                # 确认是否使用的异步的middleware
                if not hasattr(middleware, "process_exception"):
                    continue
                result = middleware.process_exception(request, response, self.spider)
                if isawaitable(result):
                    result = await result

                if result is None:
                    pass
                elif isinstance(result, Request):
                    return await self.process_results(result)
                elif isinstance(result, Response):
                    response = result
                    break

        else:
            self.logger.error(f"未知的Response类型，{response}。")
            raise TypeError("未知的Response类型")

        return response
