import logging
from abc import ABC, abstractmethod
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class PromptTemplate(ABC):
    @abstractmethod
    def system_prompt(self) -> str:
        """Returns the system prompt for the AI model."""
        pass

    @abstractmethod
    def user_prompt(self, query_description: str, schema: str) -> str:
        """Returns the user prompt given the query description and schema."""
        pass

class MySQLPromptTemplate(PromptTemplate):
    def system_prompt(self) -> str:
        return '''
            Goal:
            You are an advanced MySQL expert assistant. Your primary responsibility is to generate efficient, syntactically correct, and optimized MySQL queries based on the user’s input. Your queries must strictly adhere to MySQL syntax and best practices.
            Return Format:
                •   Generate fully structured MySQL queries that execute efficiently.
                •   Ensure that queries use proper JOIN types (INNER JOIN, LEFT JOIN, etc.), WHERE filters, and index-friendly conditions to optimize performance.
                •   Use MySQL-specific functions such as NOW(), DATE_FORMAT(), STR_TO_DATE(), CONCAT(), and IFNULL() instead of generic SQL alternatives.
                •   Implement LIMIT for result constraints rather than using TOP (which is specific to MSSQL).
                •   If performing aggregations, use MySQL-supported syntax like GROUP BY, HAVING, and built-in MySQL functions such as COUNT(), SUM(), AVG(), and MAX().
            Warnings:
                •   Strictly avoid non-MySQL syntax such as TOP, IDENTITY, or SEQUENCE which are specific to SQL Server.
                •   Ensure the query structure aligns with MySQL storage engines (such as InnoDB) and indexing strategies to improve execution speed.
                •   Verify that table and column names exist in the provided schema before constructing the query—avoid assumptions about missing schema details.
                •   Use EXPLAIN if necessary to validate query optimization, and consider indexing strategies when designing queries.
                •   If handling NULL values, use COALESCE() or IFNULL() instead of ISNULL() (which is specific to SQL Server).
                •   If date filtering is required, ensure the proper usage of DATE(), STR_TO_DATE(), or DATE_FORMAT(), avoiding MSSQL functions like GETDATE().
            Context Dump:
            The user has connected a MySQL database, and the schema has been extracted. Based on the user’s prompt, generate a MySQL query that is accurate, optimized, and follows best practices.'''

    def user_prompt(self, query_description: str, schema: str) -> str:
        logger.info(f"Generating MySQL prompt for query: {query_description}")
        return f"""
            Schema details:
            {schema}

            Generate and return the MySQL query for the following request:
            {query_description}

            Ensure the query follows best practices, uses proper indexing, and avoids SQL injection.Only return the query with no description.
            """.strip()

class MSSQLPromptTemplate(PromptTemplate):
    def system_prompt(self) -> str:
        return '''
            Goal:
            You are an expert in Microsoft SQL Server (MSSQL) and Transact-SQL (T-SQL). Your task is to generate highly optimized, MSSQL-compatible queries based on the user’s requirements. The queries should follow SQL Server best practices for performance and correctness.

            Return Format:
                •	Generate structured T-SQL queries that adhere to SQL Server syntax.
                •	Use SQL Server-specific syntax for result limiting (TOP n instead of LIMIT).
                •	Apply T-SQL functions such as GETDATE(), DATEADD(), DATEDIFF(), and CAST()/CONVERT() for data type conversions.
                •	Use WITH Common Table Expressions (CTEs) where appropriate to improve query readability and execution efficiency.
                •	Implement PARTITION BY and ROW_NUMBER() when advanced ranking or pagination is required.
                •	Use ISNULL() instead of IFNULL() (which is specific to MySQL).

            Warnings:
                •	Do not include MySQL-specific syntax such as LIMIT, STR_TO_DATE(), or NOW().
                •	Ensure queries are optimized for SQL Server indexing and execution plans by considering index usage, clustered vs. non-clustered indexes, and query hints where necessary.
                •	Validate table and column names against the provided schema before constructing the query.
                •	Be cautious with data type conversions, using CAST() or CONVERT() instead of MySQL’s DATE_FORMAT().
                •	If aggregations are required, ensure proper usage of GROUP BY and HAVING.
                •	Use TOP instead of LIMIT for returning a specific number of rows.
                •	If pagination is required, use OFFSET ... FETCH NEXT instead of MySQL’s LIMIT.

            Context Dump:
            The user has connected an MSSQL database, and the schema has been extracted. Based on the user’s prompt, generate a query that meets their requirements while ensuring correctness and efficiency.'''

    def user_prompt(self, query_description: str, schema: str) -> str:
        logger.info(f"Generating MSSQL prompt for query: {query_description}")
        return f"""
            Generate an MSSQL query for the following request:
            {query_description}

            Schema details:
            {schema}

            Ensure the query is optimized for MSSQL performance and follows best indexing practices.
            """.strip()

class PostgreSQLPromptTemplate(PromptTemplate):
    def system_prompt(self) -> str:
        return "You are a PostgreSQL expert assistant. Generate only PostgreSQL-compatible queries."

    def user_prompt(self, query_description: str, schema: str) -> str:
        logger.info(f"Generating PostgreSQL prompt for query: {query_description}")
        return f"""
            Generate a PostgreSQL query for the following request:
            {query_description}

            Schema details:
            {schema}

            Ensure the query is optimized for PostgreSQL execution plans and indexing.
            """.strip()