# tests/test_time_counter.py

import asyncio
import time

from src.nhm_spider.utils.time_counter import time_limit


def test_time_limit_synchronous_delay():
    @time_limit(seconds=2, display=False)
    def example_function():
        return "finished"

    start_time = time.time()
    result = asyncio.run(example_function())
    end_time = time.time()

    assert result == "finished"
    assert end_time - start_time >= 2  # Ensure delay occurs


def test_time_limit_asynchronous_delay():
    @time_limit(seconds=3, display=False)
    async def example_coroutine():
        return "completed"

    start_time = time.time()
    result = asyncio.run(example_coroutine())
    end_time = time.time()

    assert result == "completed"
    assert end_time - start_time >= 3  # Ensure delay occurs


def test_time_limit_no_delay():
    @time_limit(seconds=0.5, display=False)
    def quick_function():
        return "done"

    start_time = time.time()
    result = asyncio.run(quick_function())
    end_time = time.time()

    assert result == "done"
    assert end_time - start_time < 1  # Verify no unnecessary delays


def test_time_limit_timeout():
    @time_limit(seconds=0, display=False, timeout=2)
    async def slow_coroutine():
        await asyncio.sleep(5)
        return "error"

    try:
        asyncio.run(slow_coroutine())
    except asyncio.TimeoutError:
        assert True  # Ensure timeout exception is raised
    else:
        assert False, "Expected TimeoutError was not raised"


def test_time_limit_with_logging_behavior(caplog):
    @time_limit(seconds=1, display=True)
    async def example_log_function():
        return "logged"

    with caplog.at_level("INFO"):
        asyncio.run(example_log_function())

    log_messages = [record.message for record in caplog.records]
    assert any("cost time" in message for message in log_messages)
