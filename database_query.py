import re
import logging
from global_cache import GLOBAL_QUERY_CACHE  # Global persistent cache instance
from query_optimizer import QueryOptimizer
from rate_limiter import RateLimiter
from prompt_templates import MySQLPromptTemplate

class AIDatabaseQuery:
    """
    Handles SQL generation (via an AI client) and execution.
    """
    def __init__(self, ai_client, db_type: str):
        self.db_type = db_type.lower()
        self.ai_client = ai_client
        self.query_cache = GLOBAL_QUERY_CACHE  # Use the global cache instance
        self.rate_limiter = RateLimiter(30)
        self.optimizer = QueryOptimizer()
        self.logger = logging.getLogger(__name__)

    @RateLimiter(calls_per_minute=5, period=60)
    def generate_query(self, user_input: str, schema_config: dict, max_tokens: int = 150) -> dict:
        # Use the appropriate prompt template based on the database type.
        if self.db_type == "mysql":
            prompt_template = MySQLPromptTemplate()
        else:
            raise ValueError("Unsupported database type for query generation.")
        system_msg = prompt_template.system_prompt()
        user_msg = prompt_template.user_prompt(user_input, schema_config)
        if self.query_cache.get(user_input, self.db_type):
            self.logger.info("Returning cached query.")
            return {"query": self.query_cache.get(user_input, self.db_type), "latency": None, "usage": None}
        result = self.ai_client.generate_query(user_msg, max_tokens, system_prompt=system_msg)
        generated_query = result["query"]
        latency = result["latency"]
        usage = result["usage"]
        optimized_query = self.optimizer.optimize(generated_query, self.db_type)
        if not self.optimizer.validate(optimized_query):
            raise ValueError("Query validation failed")
        self.query_cache.set(user_input, self.db_type, optimized_query)
        self.logger.info("Generated and optimized query: %s", optimized_query)
        return {"query": optimized_query, "latency": latency, "usage": usage}

    def execute_query(self, connection, query: str):
        # Extract SQL code if wrapped in markdown syntax.
        match = re.search(r"```sql\s*(.*?)\s*```", query, re.DOTALL | re.IGNORECASE)
        sql_code = match.group(1).strip() if match else query.strip()
        self.logger.info("Executing SQL query: %s", sql_code)
        cursor = connection.cursor()
        cursor.execute(sql_code)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        return rows, columns