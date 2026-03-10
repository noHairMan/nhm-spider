from nhmspider import Item
from nhmspider.core.spider import Spider
from nhmspider.pipeline.interface import PipelineAbc


class Pipeline(PipelineAbc):
    def open_spider(self, spider: Spider) -> None:
        return

    def process_item(self, item: Item, spider: Spider) -> Item:
        return item

    def close_spider(self, spider: Spider) -> None:
        return
