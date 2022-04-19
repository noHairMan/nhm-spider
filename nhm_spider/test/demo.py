from nhm_spider import Item, Spider, Request, Field, CrawlerProcess
from nhm_spider.pipeline import Pipeline


class MpItem(Item):
    page = Field()


class MpPipeline(Pipeline):
    async def process_item(self, item, spider):
        return item


class MpSpider(Spider):
    name = "MpSpider"
    custom_settings = {
        "USE_SESSION": True,
        "CLEAR_COOKIE": False,
        "CONCURRENT_REQUESTS": 4,
        "DEFAULT_REQUEST_HEADER": {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/83.0.4103.97 Safari/537.36',
        },
        "ENABLED_PIPELINE": [
            # MpPipeline,
        ],
        # "RUN_FOREVER": True,
        # "RUN_LOOP_INTERVAL": 5,
        "DEBUG": True,
    }

    def __init__(self):
        super(MpSpider, self).__init__()
        self.start_url = "http://www.mp.cc/search"
        self.page_url = "http://www.mp.cc/search/{}"

    async def start_request(self):
        request = Request(self.start_url, self.parse)
        yield request

    def parse(self, response):
        page_info = response.xpath('//a[@class="number"][last()]/text()').get("0")
        total_page = int(page_info)
        for page in range(1, total_page + 1):
            request = Request(self.page_url.format(page), self.parse_page)
            request.meta["page"] = page
            yield request

    def parse_page(self, response):
        item = MpItem({"page": response.meta["page"]})
        yield item


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(MpSpider)
    process.start()
