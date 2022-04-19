# -*- coding: utf-8 -*-
"""
    下载中间件接口类
    
    @Time : 2022/4/19 15:09
    @Author : noHairMan
    @File : interface.py
    @Project : nhm-spider
"""
from abc import ABC, abstractmethod
from typing import Union

from nhm_spider.http.request import Request
from nhm_spider.http.response import Response
from nhm_spider.exceptions import NhmException
from nhm_spider.spider.base import SpiderAbc


class DownloadMiddlewareAbc(ABC):
    @abstractmethod
    def open_spider(self, spider: SpiderAbc) -> None:
        """
        启动爬虫时，初始化中间件的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """

    @abstractmethod
    def process_request(self, request: Request, spider: SpiderAbc) -> Union[Request, Response, None]:
        """
        请求发送前的拦截器，可以在此添加对request的处理

        :param request: request对象
        :type request: scrapy.http.request.Request
        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 请求 或 响应 对象。
                1. 当返回none时，会继续进行之后的中间件处理
                2. 当返回response时，会直接返回当前response对象，不会继续发送请求。
                3. 当返回request时，会将此请求重新添加到request队列中，重新进行调度。
                默认返回：none。
        :rtype: Union[Request, Response, None]
        """

    @abstractmethod
    def process_response(self, request: Request, response: Response, spider: SpiderAbc) -> \
            Union[Request, Response, None]:
        """
        请求返回后的拦截器，可以在此添加对response的处理

        :param request: request对象
        :type request: scrapy.http.request.Request
        :param response: response对象
        :type response: scrapy.http.response.Response
        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 请求 或 响应 对象。
                1. 当返回none时，会丢弃当前request。
                2. 当返回response时，会继续进行之后的中间件处理。
                3. 当返回request时，会将此请求重新添加到request队列中，重新进行调度。
                默认返回：response。
        :rtype: Union[Request, Response, None]
        """

    @abstractmethod
    def process_exception(self, request: Request, exception: NhmException, spider: SpiderAbc) -> \
            Union[Request, Response, NhmException, None]:
        """
        下载异常的拦截器，可以在此添加对exception的处理

        :param request: request对象
        :type request: scrapy.http.request.Request
        :param exception: exception对象
        :type exception: scrapy.exceptions.NhmException
        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 请求 或 响应 对象。
                1. 当返回none时，会丢弃request。
                2. 当返回response时，会跳过异常处理，直接返回当前response。
                3. 当返回request时，会将此请求重新添加到request队列中，重新进行调度。
                4. 当返回exception时，会继续进行之后中间件的处理。
                默认返回：response。
        :rtype: Union[Request, Response, None]
        """

    @abstractmethod
    def close_spider(self, spider: SpiderAbc) -> None:
        """
        关闭爬虫时，退出中间件的方法

        :param spider: spider对象
        :type spider: scrapy.spider.base.SpiderAbc
        :return: 无返回值
        :rtype: None
        """
