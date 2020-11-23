import aiomysql

from nhm_spider.common.log import get_logger
from nhm_spider.pipeline.base import AsyncPipeline


class DeprecateTmPipeline(AsyncPipeline):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.mysql_param = {
            "host": "127.0.0.1",
            "port": 3306,
            "db": "asset",
            "user": "crawler",
            "password": "Crawler2020!"
        }
        self.pool: aiomysql.Pool = None
        self.count = 0

    async def open_spider(self, spider):
        max_size = spider.settings.get("CONCURRENT_REQUESTS")
        self.pool = await aiomysql.create_pool(maxsize=max_size, **self.mysql_param)
        self.logger.info(f"Connect to mysql server {self.pool}.")

    async def process_item(self, item, spider):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.cursors.DictCursor) as cursor:
                # self.logger.info(pformat(item))
                self.count += 1
        return item

    async def close_spider(self, spider):
        self.pool.close()
        await self.pool.wait_closed()
        self.logger.info(f"Crawled total {self.count} items.")
