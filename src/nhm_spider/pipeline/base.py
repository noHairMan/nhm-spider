from nhm_spider import Item
from nhm_spider.pipeline.interface import PipelineAbc
from nhm_spider.spider.base import Spider


class Pipeline(PipelineAbc):
    def open_spider(self, spider: Spider) -> None:
        return

    def process_item(self, item: Item, spider: Spider) -> Item:
        return item

    def close_spider(self, spider: Spider) -> None:
        return
