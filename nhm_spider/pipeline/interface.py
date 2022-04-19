# -*- coding: utf-8 -*-
"""
    管道类接口
    
    @Time : 2022/4/19 15:46
    @Author : noHairMan
    @File : interface.py
    @Project : nhm-spider
"""
from abc import ABC, abstractmethod

from nhm_spider.item import Item
from nhm_spider.spider.interface import SpiderAbc


class PipelineAbc(ABC):
    @abstractmethod
    def open_spider(self, spider: SpiderAbc) -> None:
        """
        启动爬虫时，初始化管道的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """

    @abstractmethod
    def process_item(self, item: Item, spider: SpiderAbc) -> Item:
        """
        爬虫返回的item数据处理方法。

        :param item: spider返回的item，采集到的数据的载体。
        :type item: nhm_spider.item.Item
        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 返回数据的item对象，
        :rtype: nhm_spider.item.Item
        """

    @abstractmethod
    def close_spider(self, spider: SpiderAbc) -> None:
        """
        关闭爬虫时，退出管道的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """
