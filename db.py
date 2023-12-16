from loguru import logger
from psycopg_pool import ConnectionPool

import config

connection_string = f'user={config.db_user} password={config.db_password} host={config.db_host} dbname={config.db_name}'

connection_pool = ConnectionPool(connection_string)


def get_db():
    db = connection_pool.getconn()
    try:
        yield db
    finally:
        connection_pool.putconn(db)


def init():
    try:
        with connection_pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS item (
                    id SERIAL PRIMARY KEY,
                    message TEXT NOT NULL
                )
            ''')

            cursor.execute(f'''
            CREATE OR REPLACE FUNCTION item_notify()
                RETURNS trigger AS
            $$
            BEGIN
                PERFORM pg_notify('{config.notify_channel}', to_json(NEW)::text);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            ''')

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
