from abc import ABC, abstractmethod
import redis
import pandas as pd
import pickle
from functools import wraps
from typing import Callable, Any, Optional
from loguru import logger


class Datahook(ABC):
    def __init__(self):
        self.redis = redis.Redis(host="localhost", port=6379, db=0)

    @staticmethod
    def cache_df(ttl: int = 3600):
        """Decorator for caching DataFrame results in Redis."""

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(instance: "Datahook", *args, **kwargs):
                if not kwargs.get("cache", False):
                    return func(instance, *args, **kwargs)

                cache_key = (
                    f"{instance.__class__.__name__}:{func.__name__}:{str(kwargs)}"
                )

                try:
                    cached_data = instance.redis.get(cache_key)
                    if cached_data:
                        logger.info(f"Cache hit for {cache_key}")
                        return pickle.loads(cached_data)  # type: ignore

                    result = func(instance, *args, **kwargs)
                    if not isinstance(result, pd.DataFrame):
                        logger.warning(f"Result is not a DataFrame, skipping cache")
                        return result

                    instance.redis.setex(cache_key, ttl, pickle.dumps(result))
                    logger.info(f"Cached DataFrame for {cache_key}")

                    return result
                except redis.RedisError as e:
                    logger.warning(f"Redis error: {e}. Returning uncached data.")
                    return func(instance, *args, **kwargs)

            return wrapper

        return decorator

    @abstractmethod
    def get_data(self, **kwargs) -> pd.DataFrame:
        """Get processed data."""
        pass

    @abstractmethod
    def get_raw_data(self, **kwargs) -> pd.DataFrame:
        """Get raw data."""
        pass
