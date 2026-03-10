from typing import Union

from nhm_spider.core.spider import Spider
from nhm_spider.download_middleware.interface import DownloadMiddlewareAbc
from nhm_spider.exceptions import NhmException
from nhm_spider.http.request import Request
from nhm_spider.http.response import Response


class DownloadMiddleware(DownloadMiddlewareAbc):
    def open_spider(self, spider: Spider) -> None:
        return None

    def process_request(
        self,
        request: Request,
        spider: Spider,
    ) -> Union[Request, Response, None]:
        return None

    def process_response(
        self,
        request: Request,
        response: Response,
        spider: Spider,
    ) -> Union[Request, Response, None]:
        return response

    def process_exception(
        self,
        request: Request,
        exception: NhmException,
        spider: Spider,
    ) -> Union[Request, Response, NhmException, None]:
        return None

    def close_spider(self, spider: Spider) -> None:
        return None
