from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_batch
import logging

class PostgresConnector:
    def __init__(self, dbname: str, user: str, password: str, host: str = "localhost", port: str = "5432"):
        """Initialize the PostgreSQL connection."""
        logging.info(f"PostgresConnector - Connecting to database {dbname} hosted on {host} at port {port} ")
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cursor = self.conn.cursor()

    def delete_from_table(self, table: str, column: str, value:str) -> None:
        """
        Delete rows from the given table where the specified column matches the provided value.

        Args:
            table (str): The name of the table to delete from
            column (str): The column name on which to filter
            value (str): The value to match in the specified column.
        """
        try:
            query = f"DELETE FROM {table} WHERE {column} = '{value}'"
            self.cursor.execute(query)
            self.conn.commit()
            logging.info(f"PostgresConnector - executed query {query}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"PostgresConnector - Error deleting from {table}: {e}")

    def insert_data(self, table: str, data: list[BaseModel], batch_size: int = 100) -> None:
        """
        Insert a list of Pydantic models into the given table using execute_batch.

        Args:
            table (str): The name of the table to insert data into
            data (list[BaseModel]): A list of Pydantic model instances containing the data
            batch_size (int, optional): Number of records to insert per batch. Defaults to 100.
        """
        if not data:
            logging.error("PostgresConnector - No data provided for insertion.")
            raise
        
        try:
            columns = data[0].model_dump().keys()
            placeholders = ", ".join(["%s"] * len(columns))

            values = [tuple(item.model_dump().values()) for item in data]
            
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES  ({placeholders})"
            execute_batch(self.cursor, query, values, page_size=batch_size)
            self.conn.commit()
            logging.info(f"PostgresConnector - Inserted {len(data)} rows into {table} in batches of {batch_size}")
        
        except Exception as e:
            self.conn.rollback()
            logging.error(f"PostgresConnector - Error inserting into {table}: {e}")

    def get_db_row_count(self, table: str,  column: str, value:str) -> int:
        """
        Retrieve the row count from the specified table in the database.

        Args:
            table (str): The name of the table to count rows from.

        Returns:
            int: The number of rows in the table.
        """
        try:
            query = f"SELECT COUNT(*) FROM {table}  WHERE {column} = '{value}'"
            self.cursor.execute(query)
            row_count = self.cursor.fetchone()[0]
            logging.info(f"PostgresConnector - Retrieved row count: {row_count} from table {table}")
            return row_count
        except Exception as e:
            logging.error(f"PostgresConnector - Error retrieving row count from {table}: {e}")
            raise

    def close_connection(self):
        """
        Close the database connection.
        """

        self.cursor.close()
        self.conn.close()
        logging.info(f"PostgresConnector - Connection closed")