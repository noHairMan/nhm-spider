from abc import ABC, abstractmethod

from nhmspider.core.spider import Spider
from nhmspider.item.base import Item


class PipelineAbc(ABC):
    @abstractmethod
    def open_spider(self, spider: Spider) -> None:
        """
        启动爬虫时，初始化管道的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """

    @abstractmethod
    def process_item(self, item: Item, spider: Spider) -> Item:
        """
        爬虫返回的item数据处理方法。

        :param item: spider返回的item，采集到的数据的载体。
        :type item: nhmspider.item.Item
        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 返回数据的item对象，
        :rtype: nhmspider.item.Item
        """

    @abstractmethod
    def close_spider(self, spider: Spider) -> None:
        """
        关闭爬虫时，退出管道的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """
