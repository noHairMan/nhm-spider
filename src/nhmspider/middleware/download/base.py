from typing import Union

from nhmspider.core.spider import Spider
from nhmspider.exceptions import NhmException
from nhmspider.http.request import Request
from nhmspider.http.response import Response
from nhmspider.middleware.download.interface import DownloadMiddlewareAbc


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
