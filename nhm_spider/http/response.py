import json

from scrapy import Selector


class Response:
    def __init__(self, url=None, request=None, text=None, response=None, status=None, headers=None):
        self.url = url
        self.request = request
        self.text = text
        self.__response = response
        self.__selector = Selector(text=text)
        self.meta = request.meta
        self.status = status
        self.headers = headers

    def xpath(self, xpath_string):
        return self.__selector.xpath(xpath_string)

    def json(self):
        return json.loads(self.text)

    def __str__(self):
        return f"<{self.request.method.upper()} {self.url}>"
