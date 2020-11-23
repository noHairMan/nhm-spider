from asyncio import PriorityQueue

from nhm_spider.common.log import get_logger


class SpiderPriorityQueue(PriorityQueue):
    def __init__(self, maxsize=0, loop=None):
        self._finished = None
        self.logger = get_logger(self.__class__.__name__)
        super(SpiderPriorityQueue, self).__init__(maxsize=maxsize, loop=loop)

    def close(self):
        """
        封装私密方法，调用此方法会释放 asyncio.join() 的结果
        """
        self.logger.info(f"当前剩余请求{self.qsize()}。")
        self._finished.set()
