from urllib.parse import urljoin, urlparse

from nhmspider import CrawlerProcess, Field, Item, Request, Response, Spider


def get_root_domain(url):
    parsed = urlparse(url)
    netloc = parsed.netloc
    if ":" in netloc:
        netloc = netloc.split(":")[0]
    # For .onion domains and regular domains, get the root
    parts = netloc.split(".")
    if len(parts) > 1:
        return ".".join(parts[-2:])
    return netloc


class MclarenItem(Item):
    name = Field()
    depth = Field()

    url = Field()
    title = Field()
    content = Field()
    publish_time = Field()
    links = Field()
    screenshot = Field()
    update_time = Field()


class GithubSpider(Spider):
    name = "MpSpider"
    custom_settings = {
        "PROXY": "socks5://127.0.0.1:9050",
        "DEPTH_LIMIT": 1,
        "DEBUG": False,
        "DEFAULT_DOWNLOADER_CLASS": "nhmspider.core.downloader.PlaywrightDownloader",
        "BROWSER_TYPE": "firefox",
        "REQUEST_TIMEOUT": 120,
    }
    start_urls = ["https://github.com/fastfire/deepdarkCTI/blob/main/ransomware_gang.md"]

    def parse(self, response: Response):
        for tr in response.xpath('//table[@tabindex="0"]/tbody/tr'):
            name = tr.xpath("./td[1]/a/text()").get()
            url = tr.xpath("./td[1]/a/@href").get()
            status = tr.xpath("./td[2]/text()").get()
            if status == "OFFLINE":
                continue
            yield Request(url, callback=self.parse_domain, proxy=self.settings.get_string("PROXY"), meta={"name": name})

    def parse_domain(self, response: Response):
        name: str = response.meta["name"]
        depth: int = response.meta.get("depth", 1)

        item = MclarenItem(
            name=name,
            depth=depth,
            url=response.request.url,
            # title=forum["title"],
            # content=response.text,
        )
        yield item
        links = response.xpath("//body//a/@href").getall()
        final_links = []
        base_root_domain = get_root_domain(response.url)

        if depth < self.settings["DEPTH_LIMIT"]:
            for link in links:
                url_obj = urlparse(link)
                final_link = link if url_obj.netloc else urljoin(response.url, link)

                link_root_domain = get_root_domain(final_link)
                if link_root_domain != base_root_domain:
                    continue
                final_links.append(final_link)
                request = Request(
                    final_link,
                    callback=self.parse_domain,
                    proxy=self.settings.get_string("PROXY"),
                    meta={"name": name, "depth": depth + 1},
                )
                yield request


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(GithubSpider)
    process.start()
