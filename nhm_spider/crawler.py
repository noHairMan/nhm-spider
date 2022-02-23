# -*- coding: utf-8 -*-
"""
    采集管理
    
    @Time : 2022/2/23 15:58
    @Author : noHairMan
    @File : crawler.py
    @Project : nhm-spider
"""
import asyncio
from inspect import isawaitable

from nhm_spider.common.time_counter import time_limit
from nhm_spider.core.downloader import Downloader
from nhm_spider.core.scheduler import Scheduler
from nhm_spider.exceptions import NoCrawlerError
from nhm_spider.utils.signal import SignalManager


class Crawler:
    def __init__(self, spider_class):
        self.spider = spider_class.from_crawler(crawler=self)
        self.downloader = Downloader(self.spider)
        self.scheduler = Scheduler(self.spider)

        self.concurrent_requests: int = self.spider.settings.get_int("CONCURRENT_REQUESTS", 8)

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
        for pipeline in self.scheduler.enabled_pipeline:
            # 确认是否使用的异步的pipeline
            if not hasattr(pipeline, "open_spider"):
                continue
            pip = pipeline.open_spider(self.spider)
            if isawaitable(pip):
                await pip

        # init download middleware
        for middleware in self.scheduler.enabled_download_middleware:
            # 确认是否使用的异步的middleware
            if not hasattr(middleware, "open_spider"):
                continue
            mid = middleware.open_spider(self.spider)
            if isawaitable(mid):
                await mid

    async def _close_crawler(self):
        """
        退出crawler的准备操作
        """
        # clear pipeline
        for pipeline in self.scheduler.enabled_pipeline:
            # 确认是否使用的异步的pipeline
            if not hasattr(pipeline, "close_spider"):
                continue
            pip = pipeline.close_spider(self.spider)
            if isawaitable(pip):
                await pip

        # clear download middleware
        for middleware in self.scheduler.enabled_download_middleware:
            # 确认是否使用的异步的middleware
            if not hasattr(middleware, "close_spider"):
                continue
            mid = middleware.close_spider(self.spider)
            if isawaitable(mid):
                await mid

    async def crawl(self):
        """
        协程主程序
        """
        # todo: 应打印初始化了哪些模块。

        await self._open_crawler()

        tasks = []
        try:
            # 初始化
            results = self.spider.start_request()
            await self.scheduler.process_results(results)
            for _ in range(self.concurrent_requests):
                task = asyncio.create_task(self.scheduler.process(self.downloader))
                tasks.append(task)

            # 阻塞并等待所有任务完成   todo: heartbeat应放置到单独模块中去
            tasks.append(asyncio.create_task(self.scheduler.heartbeat()))
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
