import psycopg2
import binascii
from psycopg2.extras import DictCursor


# function to convert bytea data to string
def data_to_str(s):
    if s is None:
        return None
    if isinstance(s, bytes):
        return binascii.hexlify(s).decode('utf-8')
    return str(s)


class Postgres:
    def __init__(self, port, user, password, database, host='localhost'):
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = psycopg2.connect(
            port=port,
            database=database,
            user=user,
            password=password,
            host=host
        )

    def dict_query(self, sql):
        # create a cursor with DictCursor and custom functions
        cursor = self.conn.cursor(cursor_factory=DictCursor)
        psycopg2.extensions.register_adapter(bytes, data_to_str)

        # execute the SELECT statement
        cursor.execute(
            sql)
        data = [dict(row) for row in cursor.fetchall()]
        # close the cursor and connection
        cursor.close()

        # return all the data from the table as dictionaries
        return data

    def close(self):
        self.conn.close()

    def create_table(self, table_name, columns, primary_key):
        cursor = self.conn.cursor()
        # cursor.execute(f"DROP TABLE IF EXISTS public.{table_name};")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS public.{table_name}({columns}, CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_key})) TABLESPACE pg_default; ALTER TABLE IF EXISTS public.{table_name} OWNER TO chain;")
        self.conn.commit()
        cursor.close()

    def insert_row(self, table_name, columns, values):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
            self.conn.commit()
        except:
            self.conn.rollback()
            cursor.close()
            raise
        cursor.close()

    def insert_rows(self, table_name, columns, values):
        cursor = self.conn.cursor()
        values = [(value,) for value in values]
        try:
            cursor.executemany(
                f"INSERT INTO {table_name} ({columns}) VALUES (%s)", values)
            self.conn.commit()
        except:
            self.conn.rollback()
            cursor.close()
            raise
        cursor.close()
