import time
import asyncio
import functools
import logging


def measure_time(name: str | None = None):

    def decorator(func):
        func_name = name or func.__name__
        log = logging.getLogger("measure_time")

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - start
                    log.info(f"[{func_name}] took {elapsed:.3f}s")

            return async_wrapper

        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - start
                    log.info(f"[{func_name}] took {elapsed:.3f}s")

            return sync_wrapper

    return decorator
