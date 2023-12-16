import psycopg2
from loguru import logger
from psycopg2 import pool

import config

connection_pool = pool.SimpleConnectionPool(minconn=1,
                                            maxconn=10,
                                            user=config.db_user,
                                            password=config.db_password,
                                            host=config.db_host,
                                            database=config.db_name)


def get_db():
    db = connection_pool.getconn()
    try:
        yield db
    finally:
        connection_pool.putconn(db)


def init():
    conn = connection_pool.getconn()

    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS item (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL
            )
        ''')

        cursor.execute('''
        CREATE OR REPLACE FUNCTION item_notify()
            RETURNS trigger AS
        $$
        BEGIN
            PERFORM pg_notify(%s, to_json(NEW)::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        ''', [config.notify_channel])

        cursor.execute('''
        CREATE OR REPLACE TRIGGER item_update
            AFTER INSERT OR UPDATE ON item
            FOR EACH ROW
        EXECUTE PROCEDURE item_notify();
        ''')

        conn.commit()

    except Exception as e:
        logger.exception(e)
        raise
    finally:
        connection_pool.putconn(conn)
