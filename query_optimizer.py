import logging
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Handles query optimization and validation"""

    def __init__(self):
        self.prohibited_patterns = ['DROP', 'DELETE', 'TRUNCATE', 'GRANT', 'REVOKE', 'ALTER']
        logger.info("QueryOptimizer initialized with prohibited patterns: %s", self.prohibited_patterns)

    def optimize(self, query: str, db_type: str) -> str:
        optimized = query.strip(';') + ';'
        logger.info("Optimized query: %s", optimized)
        return optimized

    def validate(self, query: str) -> bool:
        for pattern in self.prohibited_patterns:
            if pattern in query.upper():
                logger.warning("Query validation failed: found prohibited pattern %s", pattern)
                return False
        logger.info("Query validation passed.")
        return True