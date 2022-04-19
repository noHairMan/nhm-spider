# -*- coding: utf-8 -*-
"""
    爬虫接口
    
    @Time : 2022/4/19 15:18
    @Author : noHairMan
    @File : interface.py
    @Project : nhm-spider
"""
from abc import ABC, abstractmethod


class SpiderAbc(ABC):
    name: str
    start_urls: list
    settings: dict
    custom_settings: dict

    @abstractmethod
    def start_request(self):
        """
        启动爬虫任务的方法，需添加启动任务到此处
        """

    @abstractmethod
    def parse(self):
        """
        start_urls里的方法的回调，处理方法。
        """

    @classmethod
    @abstractmethod
    def from_crawler(cls, crawler=None, *args, **kwargs):
        """
        创建实例的类方法
        """