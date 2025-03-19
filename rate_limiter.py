import time
import logging
import functools
from collections import deque
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class RateLimiter:
    """Decorator to enforce a maximum number of calls within a given period."""
    
    def __init__(self, calls_per_minute: int, period: float = 60.0):
        """
        Initialize the rate limiter.
        
        :param calls_per_minute: Maximum allowed calls per period.
        :param period: Time window in seconds (default is 60 seconds).
        """
        self.calls_per_minute = calls_per_minute
        self.period = period
        self.timestamps = deque()
        logger.info("RateLimiter initialized with %d calls per %.2f seconds", calls_per_minute, period)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()
            # Remove timestamps that are older than the period.
            while self.timestamps and self.timestamps[0] <= current_time - self.period:
                self.timestamps.popleft()
            
            if len(self.timestamps) >= self.calls_per_minute:
                # Calculate how long to sleep until the oldest timestamp is out of the time window.
                sleep_time = self.period - (current_time - self.timestamps[0])
                logger.warning("Rate limit reached. Sleeping for %.2f seconds.", sleep_time)
                time.sleep(sleep_time)
                current_time = time.time()  # Recalculate current time after sleep.
                while self.timestamps and self.timestamps[0] <= current_time - self.period:
                    self.timestamps.popleft()
            
            self.timestamps.append(current_time)
            return func(*args, **kwargs)
        return wrapper