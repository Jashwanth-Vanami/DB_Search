import mysql.connector
import logging

class DBConnector:
    """
    Manages database connections. Supports context management for automatic
    connection handling and closing.
    """
    def __init__(self, db_type: str, config: dict):
        self.db_type = db_type.lower()
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        if self.db_type == "mysql":
            self.connection = mysql.connector.connect(**self.config)
        elif self.db_type == "mssql":
            # Placeholder for MSSQL connection implementation.
            raise NotImplementedError("MSSQL connection not implemented.")
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        self.logger.info(f"Connected to {self.db_type} database.")
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info(f"Closed {self.db_type} database connection.")