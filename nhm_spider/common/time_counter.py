import asyncio
import time

from nhm_spider.common.log import get_logger

logger = get_logger("TimeCounter")


def time_limit(seconds: (int, float) = 0, display=False):
    """
    装饰器
    统计使用时间
    :param display: 是否打印日志
    :param seconds: 控制方法执行的最短时间，在此时间之前完成，强行等待
    """
    def outer(func):
        def wrap(*args, **kwargs):
            start_time = time.time()
            r = func(*args, **kwargs)
            end_time = time.time()
            cost_time = end_time - start_time
            if display is True:
                logger.info(f"[{func.__name__}] cost time {cost_time}")
            if seconds and cost_time < seconds:
                time.sleep(seconds - cost_time)
            return r
        return wrap
    return outer


def async_time_limit(seconds: (int, float) = 0, display=False, timeout=None):
    """
    装饰器
    统计使用时间
    :param timeout: 协程对象超时时间
    :param display: 是否打印日志
    :param seconds: 控制方法执行的最短时间，在此时间之前完成，强行等待
    """
    def outer(func):
        async def wrap(*args, **kwargs):
            start_time = time.time()
            if timeout is None:
                r = await func(*args, **kwargs)
            else:
                r = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            end_time = time.time()
            cost_time = end_time - start_time
            if display is True:
                logger.info(f"[{func.__name__}] cost time {cost_time}")
            if seconds and cost_time < seconds:
                await asyncio.sleep(seconds - cost_time)
            return r
        return wrap
    return outer
