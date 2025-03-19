import logging
import os
from connector import DBConnector
from SchemaGenerator import SchemaGenerator
from database_query import AIDatabaseQuery
from ai_clients import OpenAIClient

class DBSearch:
    """
    Orchestrates the entire flow:
      1. Extract schema (using SchemaGenerator with DBConnector)
      2. Generate a SQL query using the AI client and prompt template
      3. Execute the query and return results
    """
    def __init__(self, db_type: str):
        self.db_type = db_type.lower()
        self.logger = logging.getLogger(__name__)
        # Load DB configuration from environment variables.
        if self.db_type == "mysql":
            self.config = {
                "host": os.getenv("MYSQL_HOST"),
                "port": int(os.getenv("MYSQL_PORT", 3306)),
                "user": os.getenv("MYSQL_USER"),
                "password": os.getenv("MYSQL_PASSWORD"),
                "database": os.getenv("MYSQL_DATABASE"),
            }
        else:
            raise ValueError("Unsupported database type")
        self.ai_client = OpenAIClient(api_key=os.getenv("OPENAI_API_KEY"))
        self.database_query = AIDatabaseQuery(self.ai_client, self.db_type)

    def run(self, user_prompt: str):
        # Step 1: Generate schema using SchemaGenerator.
        schema_gen = SchemaGenerator(self.db_type, self.config)
        schema_config = schema_gen.fetch_schema()
        # Step 2: Generate SQL query.
        query_result = self.database_query.generate_query(user_prompt, schema_config)
        generated_query = query_result["query"]
        # Step 3: Execute the query using a DBConnector.
        from connector import DBConnector  # local import for clarity
        with DBConnector(self.db_type, self.config) as connection:
            rows, columns = self.database_query.execute_query(connection, generated_query)
        return {
            "generated_query": generated_query,
            "latency": query_result["latency"],
            "usage": query_result["usage"],
            "rows": rows,
            "columns": columns,
            "schema": schema_config
        }