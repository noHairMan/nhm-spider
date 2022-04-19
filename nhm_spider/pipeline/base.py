from nhm_spider import Item
from nhm_spider.pipeline.interface import PipelineAbc
from nhm_spider.spider.interface import SpiderAbc


class Pipeline(PipelineAbc):
    def open_spider(self, spider: SpiderAbc) -> None:
        return

    def process_item(self, item: Item, spider: SpiderAbc) -> Item:
        return item

    def close_spider(self, spider: SpiderAbc) -> None:
        return
