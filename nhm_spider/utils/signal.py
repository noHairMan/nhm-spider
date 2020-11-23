import signal

from nhm_spider.common.log import get_logger


class SignalManager:
    def __init__(self, request_queue):
        self.logger = get_logger(self.__class__.__name__)
        self.request_queue = request_queue

    def handler(self, signum, frame):
        # 执行后会释放 join 部分
        self.logger.warning(f"收到信号[{signum}]。")
        self.request_queue.close()

    def connect(self):
        signal.signal(signal.SIGINT, self.handler)

    def __del__(self):
        pass
