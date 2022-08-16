# -*- coding: utf-8 -*-
"""
    下载中间件基类
    
    @Time : 2022/4/19 13:44
    @Author : noHairMan
    @File : base.py
    @Project : nhm-spider
"""
from typing import Union

from nhm_spider.http.request import Request
from nhm_spider.http.response import Response
from nhm_spider.download_middleware.interface import DownloadMiddlewareAbc
from nhm_spider.exceptions import NhmException
from nhm_spider.spider.base import SpiderAbc


class DownloadMiddleware(DownloadMiddlewareAbc):
    def open_spider(self, spider: SpiderAbc) -> None:
        return None

    def process_request(self, request: Request, spider: SpiderAbc) -> Union[Request, Response, None]:
        return None

    def process_response(self, request: Request, response: Response, spider: SpiderAbc) -> \
            Union[Request, Response, None]:
        return response

    def process_exception(self, request: Request, exception: NhmException, spider: SpiderAbc) -> \
            Union[Request, Response, NhmException, None]:
        return None

    def close_spider(self, spider: SpiderAbc) -> None:
        return None
