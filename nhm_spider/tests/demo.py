from nhm_spider import Item, Spider, Engine, Request, Field


class MpItem(Item):
    page = Field()


class MpSpider(Spider):
    custom_settings = {
        "USE_SESSION": True,
        "CLEAR_COOKIE": False,
        "CONCURRENT_REQUESTS": 4,
        "DEFAULT_REQUEST_HEADER": {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/83.0.4103.97 Safari/537.36',
        },
        "ENABLED_PIPELINE": [
            # TmPipeline,
            # TmQueryPipeline,
        ],
    }

    def __init__(self):
        super(MpSpider, self).__init__()
        self.start_url = "http://www.mp.cc/search"
        self.page_url = "http://www.mp.cc/search/{}"

    async def start_request(self):
        request = Request(self.start_url, self.parse,
                          # proxy="http://127.0.0.1:8888"
                          )
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
    engine = Engine()
    engine.run(MpSpider)
