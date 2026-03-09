from nhm_spider import CrawlerProcess, Response, Spider


class GithubSpider(Spider):
    name = "MpSpider"
    custom_settings = {
        "USE_SESSION": True,
        "CLEAR_COOKIE": False,
        "CONCURRENT_REQUESTS": 4,
        "DEFAULT_REQUEST_HEADER": {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/83.0.4103.97 Safari/537.36",
        },
        "DEBUG": True,
        "DEFAULT_DOWNLOADER_CLASS": "nhm_spider.core.downloader.PlaywrightDownloader",
        "BROWSER_TYPE": "firefox",
    }
    start_urls = ["https://github.com/fastfire/deepdarkCTI/blob/main/ransomware_gang.md"]

    def parse(self, response: Response):
        print(response.text)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(GithubSpider)
    process.start()
