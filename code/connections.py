import psycopg
from psycopg.rows import tuple_row, dict_row
from psycopg.sql import SQL
from typing import Self, Union
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, GROUP_INSTANCE_ID
from loggers import logger
from time import sleep


class DBConnection:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, application_name: str = None ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.application_name = application_name
        self.conn = psycopg.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password, application_name=self.application_name)

    def reconnect(self):
        self.conn.close()
        self.conn = psycopg.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password)

    def close(self):
        self.conn.close()
        
    def exc_wrapper(func):
        def inner(self: Self, *args, **kwargs):
            # Auto-commit & rollback upon failure
            try:
                res = func(self, *args, **kwargs)
                self.conn.commit()
                return res
            except Exception as err:
                self.conn.rollback()
                raise err
        return inner
    
    def retry_wrapper(count: int = 5, delay: int = 2):
        def outer(func):
            def inner(self: Self, *args, **kwargs):
                last_err = None
                for i in range(1, count+1):
                    try:
                        res = func(self, *args, **kwargs)
                        return res
                    # Only retry for connection-specific issues e.g. OperationalError
                    except (psycopg.errors.OperationalError, psycopg.errors.InternalError) as operr:
                        logger.error(f'Error occurred. Reconnecting database and retrying transaction... ( {i} / {count} )')
                        sleep(delay)
                        try:
                            self.reconnect()
                            logger.info('reconnected!')
                        except psycopg.errors.OperationalError:
                            pass
                        last_err = operr
                logger.error('Retry attempt limit reached.')
                raise last_err
            return inner
        return outer

    @retry_wrapper(5,2)
    @exc_wrapper
    def execute(self, sql: str, parameters=[], row_factory=tuple_row):
        cur = self.conn.cursor(row_factory=row_factory)
        return cur.execute(sql, parameters)
    
    @retry_wrapper(5,2)
    @exc_wrapper
    def executemany(self, sql: str, parameters=[], row_factory=tuple_row, returning=False):
        cur = self.conn.cursor(row_factory=row_factory)
        res =  cur.executemany(sql, parameters)
        self.conn.commit()
        if returning:
            rows = []
            while True:
                rows.append(cur.fetchone()[0])
                if not cur.nextset():
                    break
            return rows
        return res
    
    @retry_wrapper(5,2)
    @exc_wrapper
    def execute_pipeline(self, queries: list[(SQL, dict)]):
        """
        Execute multiple queries in a single transaction.
        Queries must be a list of (sql_query, params) sets.
        """
        with self.conn.pipeline() as pipe:
            for (sql, params) in queries:
                self.conn.execute(sql, params)
            self.conn.commit()
    
    def __del__(self):
        self.close()
    

conn_pg = DBConnection(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, application_name=GROUP_INSTANCE_ID)

