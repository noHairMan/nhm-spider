from nhmspider.core.interface import CrawlerABC, SpiderABC
from nhmspider.http.request import Request
from nhmspider.settings.settings_manager import SettingsManager
from nhmspider.utils.log import get_logger
from nhmspider.utils.project import get_default_settings


class Spider(SpiderABC):
    name = "Spider"
    start_urls = []
    custom_settings = {}

    def start_request(self):
        for url in self.start_urls:
            request = Request(url, callback=self.parse)
            yield request

    def parse(self, response): ...
