class Pipeline:
    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        pass


class AsyncPipeline:
    async def open_spider(self, spider):
        pass

    async def process_item(self, item, spider):
        return item

    async def close_spider(self, spider):
        pass
