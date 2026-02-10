import asyncio
import time

from nhm_spider.utils.log import get_logger

logger = get_logger("TimeCounter")


def time_limit(seconds: int | float = 0, display: bool = False, timeout: bool = None):
    """
    限制异步或同步函数的执行时长的装饰器，支持延迟显示。

    该装饰器可以设置最大运行时间和函数运行完成后的等待时间。
    如函数执行时间少于指定时长，装饰器会自动延迟以补足时间。
    支持异步函数和同步函数的使用。

    Args:
        seconds (int | float): 函数运行所需的最小时间。如果函数运行时间少于此值，
            将自动延迟补足，缺省值为0。
        display (bool): 是否显示函数的运行时间日志。缺省值为False。
        timeout (bool | None): 最大超时时间。若为None，则不做超时限制；
            若设置超时时长，则超出时将引发asyncio超时相关的错误。
            缺省值为None。

    Returns:
        Callable: 返回一个可用于装饰函数的装饰器。
    """

    def outer(func):
        async def wrap(*args, **kwargs):
            start_time = time.time()
            if asyncio.iscoroutinefunction(func):
                if timeout is None:
                    r = await func(*args, **kwargs)
                else:
                    r = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            else:
                if timeout is None:
                    r = await asyncio.to_thread(func, *args, **kwargs)
                else:
                    r = await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout)
            end_time = time.time()
            cost_time = end_time - start_time
            if display:
                logger.info(f"[{func.__name__}] cost time {cost_time}")
            if seconds and cost_time < seconds:
                await asyncio.sleep(seconds - cost_time)
            return r

        return wrap

    return outer
