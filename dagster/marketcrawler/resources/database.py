from contextlib import contextmanager
import os
import psycopg
from dagster import ConfigurableResource
from typing import Generator


class DatabaseResource(ConfigurableResource):
    """A simple context manager, making sure conn.close() is called when we don't need it anymore."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @contextmanager
    def get_connection(self) -> Generator[
        psycopg.Connection,
        None,
        None,
    ]:
        conn = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
        finally:
            conn.close()


Database = DatabaseResource(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DB", "database"),
    user=os.getenv("POSTGRES_USER", "user"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)
