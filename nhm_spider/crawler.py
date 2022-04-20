# -*- coding: utf-8 -*-
"""
    爬行者
    
    @Time : 2022/2/23 15:58
    @Author : noHairMan
    @File : crawler.py
    @Project : nhm-spider
"""
import asyncio
from asyncio import Semaphore, Future, wait
from inspect import isawaitable, iscoroutine
from traceback import format_exc
from types import GeneratorType, AsyncGeneratorType
from typing import Union

from nhm_spider.http.request import Request
from nhm_spider.http.response import Response
from nhm_spider.item import Item
from nhm_spider.common.log import get_logger
from nhm_spider.common.time_counter import time_limit
from nhm_spider.core.downloader import Downloader
from nhm_spider.core.scheduler import Scheduler
from nhm_spider.exceptions import NoCrawlerError, ExceptionEnum
from nhm_spider.utils.request import request_fingerprint
from nhm_spider.utils.signal import SignalManager


class Crawler:
    def __init__(self, spider_class):
        self.logger = get_logger("Crawler")
        self.spider = spider_class.from_crawler(crawler=self)
        self.downloader = Downloader(self.spider)
        self.scheduler = Scheduler(self.spider)

        self.concurrent_requests: int = self.spider.settings.get_int("CONCURRENT_REQUESTS", 8)
        # pipeline
        enabled_pipeline = self.spider.settings.get_list("ENABLED_PIPELINE")
        self.enabled_pipeline = [cls() for cls in enabled_pipeline]
        # download middleware
        enabled_download_middleware = self.spider.settings.get_list("ENABLED_DOWNLOAD_MIDDLEWARE")
        self.enabled_download_middleware = [cls() for cls in enabled_download_middleware]
        # spider middleware
        # enabled_spider_middleware = settings.get_list("ENABLED_SPIDER_MIDDLEWARE")
        # self.enabled_spider_middleware = [cls() for cls in enabled_spider_middleware]

    @time_limit(display=True)
    def run(self):
        asyncio.run(self.crawl())

    async def _open_crawler(self):
        """
        初始化crawler
        """
        # todo: 应尝试减少某些模块的初始化次数
        await self.scheduler.open_scheduler()
        await self.downloader.open_downloader()
        await self.spider.custom_init()
        self.signal_manager = SignalManager(self.scheduler.request_queue)
        self.signal_manager.connect()

        # init pipeline
        for pipeline in self.enabled_pipeline:
            pip = pipeline.open_spider(self.spider)
            if isawaitable(pip):
                await pip

        # init download middleware
        for middleware in self.enabled_download_middleware:
            mid = middleware.open_spider(self.spider)
            if isawaitable(mid):
                await mid

    async def _close_crawler(self):
        """
        退出crawler的准备操作
        """
        # clear pipeline
        for pipeline in self.enabled_pipeline:
            pip = pipeline.close_spider(self.spider)
            if isawaitable(pip):
                await pip

        # clear download middleware
        for middleware in self.enabled_download_middleware:
            mid = middleware.close_spider(self.spider)
            if isawaitable(mid):
                await mid

    async def crawl(self):
        """
        协程主程序
        """
        def callback(future: Future):
            semaphore.release()
            exception = future.exception()
            if exception:
                raise exception

        # todo: 应打印初始化了哪些模块。

        await self._open_crawler()

        tasks = []
        semaphore = Semaphore(value=self.concurrent_requests)
        try:
            # 初始化
            results = self.spider.start_request()
            await self.process_results(results)
            # todo: heartbeat应放置到单独模块中去
            tasks.append(asyncio.create_task(self.scheduler.heartbeat()))
            while 1:
                # 强制退出时候退出循环 todo: 续判断退出到时候当前已经开始的任务是否已经执行完？
                if self.scheduler.request_queue._finished.is_set():
                    break
                # 所有任务都已经处理完时，执行退出循环
                if self.scheduler.request_queue.empty() and semaphore._value == self.concurrent_requests:
                    break
                rs = await wait([self.scheduler.next_request()], timeout=1)
                if not rs[0]:
                    continue
                request = rs[0].pop().result()
                await semaphore.acquire()
                # todo
                asyncio.create_task(self.process(request)).add_done_callback(callback)

            # 阻塞并等待所有任务完成
            await self.scheduler.request_queue.join()

            # 正常推出时执行的关闭
            success_close_task = self.spider.custom_success_close()
            if isawaitable(success_close_task):
                await success_close_task
        finally:
            await self._close_crawler()

            await self.downloader.session.close()
            # 所有task完成后，取消任务，退出程序
            for task in tasks:
                task.cancel()
            # 等待task取消完成
            await asyncio.gather(*tasks, return_exceptions=True)

            spider_close_task = self.spider.custom_close()
            if isawaitable(spider_close_task):
                await spider_close_task

            # 清理内存，消除对 RUN_FOREVER = True 时的影响
            self.scheduler.dupe_memory_queue.clear()
            tasks.clear()

            # todo: 应打印采集完成汇总的数据。

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

    async def process_request(self, obj, response):
        """
        处理单个request，将request添加到请求队列中等待调度
        """

        # 处理request对象优先级，深度优先
        obj.priority = (response.request.priority - 1) if obj.priority is None and response is not None else 0
        # 根据指纹去重
        fp = request_fingerprint(obj)
        if obj.dont_filter is True or fp not in self.scheduler.dupe_memory_queue:
            obj.fp = fp
            await self.scheduler.enqueue_request(obj)
            self.scheduler.dupe_memory_queue.add(fp)

    async def process_item(self, obj):
        """
        处理单个item，在pipeline中处理item
        """

        if not self.enabled_pipeline and self.spider.DEBUG is True:
            self.logger.info(obj)
        self.scheduler.item_count += 1
        for pipeline in self.enabled_pipeline:
            obj = pipeline.process_item(obj, self.spider)
            if isawaitable(obj):
                obj = await obj

    async def process_result_single(self, obj, response):
        """
        处理spider中返回的对象
        """
        if isinstance(obj, Request):
            await self.process_request(obj, response)
        elif isinstance(obj, Item):
            await self.process_item(obj)
        else:
            self.logger.warning(f"[yield]尚未处理的类型[{obj.__class__.__name__}]。")

    async def process(self, request):
        response = await self.download_request(request)
        if isinstance(response, Response):
            if self.spider.DEBUG is True:
                self.logger.info(f"Crawled ({response.status}) {response}.")
        else:
            # todo: 待处理非response的情况
            # 失败的请求也要调用task_done，否则无法结束。
            self.scheduler.request_queue.task_done()
            self.scheduler.request_count += 1
            return

        # todo: process_spider_in
        results = request.callback(response)
        # todo: process_spider_out 非此位置
        try:
            await self.process_results(results, response)
        except Exception:
            self.logger.error(format_exc())
        finally:
            self.scheduler.request_queue.task_done()
            self.scheduler.request_count += 1

    async def download_request(self, request):
        # process_request
        for middleware in self.enabled_download_middleware:
            result = middleware.process_request(request, self.spider)
            if isawaitable(result):
                result = await result

            if isinstance(result, Request):
                return await self.process_results(result)
            elif isinstance(result, Response):
                # 返回response则直接跳过process_request
                request = result
                break
            elif result is not None:
                self.logger.error(f"未知的对象类型，{result}。")
                raise TypeError(ExceptionEnum.TYPE_ERROR)

        # download
        if isinstance(request, Request):
            response = await self.downloader.send_request(request)
        elif isinstance(request, Response):
            response = request
        else:
            self.logger.error(f"未知的对象类型: {request}。")
            raise TypeError(ExceptionEnum.TYPE_ERROR)

        # process_response
        if isinstance(response, Response):
            for middleware in self.enabled_download_middleware:
                result = middleware.process_response(request, response, self.spider)
                if isawaitable(result):
                    result = await result

                if isinstance(result, Request):
                    return await self.process_results(result)
                elif isinstance(result, Response):
                    response = result
                    break
                elif result is not None:
                    self.logger.error(f"未知的对象类型: {result}。")
                    raise TypeError("未知的对象类型")
        elif isinstance(response, Exception):
            for middleware in self.enabled_download_middleware:
                result = middleware.process_exception(request, response, self.spider)
                if isawaitable(result):
                    result = await result

                # todo: return exception 未处理
                if isinstance(result, Request):
                    return await self.process_results(result)
                elif isinstance(result, Response):
                    response = result
                    break
                elif result is not None:
                    self.logger.error(f"未知的对象类型: {result}。")
                    raise TypeError("未知的对象类型")
        else:
            self.logger.error(f"未知的对象类型，{response}。")
            raise TypeError(ExceptionEnum.TYPE_ERROR)

        return response


class CrawlerRunner:
    def __init__(self):
        self.crawlers = []

    def crawl(self, spider_class):
        crawler = Crawler(spider_class)
        self.crawlers.append(crawler)


class CrawlerProcess(CrawlerRunner):
    def start(self):
        if not self.crawlers:
            raise NoCrawlerError("use method `CrawlerProcess.crawl` add spider class.")
        elif len(self.crawlers) == 1:
            # 只有一个爬虫任务，在主进程中运行
            crawler = self.crawlers[0]
            crawler.run()
        else:
            # todo: 使用多进程，每个进程运行单个爬虫
            pass
