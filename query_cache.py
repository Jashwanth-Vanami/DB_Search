import hashlib
import logging
from collections import OrderedDict
from typing import Optional
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class QueryCache:
    """LRU cache for generated queries using an OrderedDict for true LRU behavior."""
    
    def __init__(self, max_size: int = 100):
        self.cache = OrderedDict()
        self.max_size = max_size
        logger.info("QueryCache initialized with max_size %d", max_size)
    
    def _normalize(self, user_input: str) -> str:
        """
        Normalize the user input by stripping extra whitespace and converting to lowercase.
        This ensures that similar inputs produce the same cache key.
        """
        return user_input.strip().lower()
    
    def _generate_key(self, user_input: str, db_type: str) -> str:
        normalized_input = self._normalize(user_input)
        normalized_db_type = db_type.strip().lower()
        key = hashlib.sha256(f"{normalized_input}-{normalized_db_type}".encode()).hexdigest()
        logger.debug("Generated cache key: %s for normalized input: %s and db_type: %s", key, normalized_input, normalized_db_type)
        return key

    def get(self, user_input: str, db_type: str) -> Optional[str]:
        key = self._generate_key(user_input, db_type)
        result = self.cache.get(key)
        if result is not None:
            # Move key to the end to mark it as recently used.
            self.cache.move_to_end(key)
        logger.info("Cache get for key %s: %s", key, result)
        return result

    def set(self, user_input: str, db_type: str, query: str) -> None:
        key = self._generate_key(user_input, db_type)
        self.cache[key] = query
        self.cache.move_to_end(key)
        if len(self.cache) > self.max_size:
            evicted_key, _ = self.cache.popitem(last=False)
            logger.warning("Cache max size reached. Evicting key: %s", evicted_key)
        logger.info("Cache set for key %s with query: %s", key, query)