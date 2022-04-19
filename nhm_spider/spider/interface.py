# -*- coding: utf-8 -*-
"""
    爬虫类接口
    
    @Time : 2022/4/19 15:18
    @Author : noHairMan
    @File : interface.py
    @Project : nhm-spider
"""
from abc import ABC, abstractmethod
from typing import Union, Generator, AsyncGenerator, List, Mapping

from nhm_spider.crawler import Crawler
from nhm_spider.item import Item
from nhm_spider.http.request import Request


class SpiderAbc(ABC):
    # 爬虫类的名称，spider类的logger会使用此名称创建
    name: str
    # 启动爬虫的初始链接，该数组中的链接会以get方式发送请求。
    start_urls: List[str]
    # 当前spider的专用配置
    custom_settings: Mapping

    @abstractmethod
    def start_request(self) -> Union[Generator[Union[Request, Item], None, None],
                                     AsyncGenerator[Union[Request, Item], None]]:
        """
        启动爬虫任务的方法，需添加启动任务到此处

        :return: 生成器 或 异步生成器，可返回request对象或者item对象，返回的request对象会被添加到请求队列中等待调度，
                 返回的item会被放到pipeline中进行处理，如入库，发布到消息队列等。
        :rtype: Union[Generator[Union[Request, Item], None, None], AsyncGenerator[Union[Request, Item], None]]
        """

    @abstractmethod
    def parse(self) -> Union[Generator[Union[Request, Item], None, None],
                             AsyncGenerator[Union[Request, Item], None]]:
        """
        start_urls里的方法的回调，处理方法。

        :return: 生成器 或 异步生成器，可返回request对象或者item对象，返回的request对象会被添加到请求队列中等待调度，
                 返回的item会被放到pipeline中进行处理，如入库，发布到消息队列等。
        :rtype: Union[Generator[Union[Request, Item], None, None], AsyncGenerator[Union[Request, Item], None]]
        """

    @classmethod
    @abstractmethod
    def from_crawler(cls, crawler: Crawler = None, *args, **kwargs):
        """
        创建当前spider类的实例，通过此方法创建，会对爬虫进行一系列必须的初始化操作，请不要自行进行spider类实例的创建。

        :param crawler: crawler对象
        :type crawler: nhm_spider.crawler.Crawler
        :return: 返回当前类的实例对象。
        :rtype: cls, nhm_spider.spider.interface.SpiderAbc
        """