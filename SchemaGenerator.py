import logging
from connector import DBConnector

class SchemaGenerator:
    """
    Dynamically extracts tables, columns, and relationships from the database.
    """
    def __init__(self, db_type: str, config: dict):
        self.db_type = db_type.lower()
        self.config = config
        self.schema = {}
        self.relationships = {}
        self.logger = logging.getLogger(__name__)

    def fetch_schema(self):
        if self.db_type != "mysql":
            raise NotImplementedError("Schema generation is only implemented for MySQL.")
        with DBConnector(self.db_type, self.config) as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables = [table[0] for table in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"DESCRIBE {table};")
                columns = [column[0] for column in cursor.fetchall()]
                self.schema[table] = {"columns": columns, "primary_key": None}
                cursor.execute(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY';")
                primary_keys = [row[4] for row in cursor.fetchall()]
                if primary_keys:
                    self.schema[table]["primary_key"] = primary_keys
            cursor.execute("""
                SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL;
            """)
            foreign_keys = cursor.fetchall()
            for row in foreign_keys:
                table_name, column_name, _, ref_table, ref_column = row
                if table_name not in self.relationships:
                    self.relationships[table_name] = []
                self.relationships[table_name].append({
                    "column": column_name,
                    "references": {"table": ref_table, "column": ref_column}
                })
            cursor.close()
            self.logger.info("Schema and relationships fetched successfully.")
            return {"tables": self.schema, "relationships": self.relationships}