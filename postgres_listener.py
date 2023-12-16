import asyncio
import psycopg2
from loguru import logger
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json

import config


class PostgresListener:
    def __init__(self, host: str, dbname: str, user: str, password: str, channel: str):
        self.disconnected = True
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.channel = channel
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(host=self.host, dbname=self.dbname, user=self.user, password=self.password)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"LISTEN {self.channel};")
        self.subscribers = []

        try:
            self.event_loop = asyncio.get_running_loop()
            logger.info("Attached to running event loop")
        except RuntimeError as e:
            logger.error("Could not attach to running event loop")
            raise e

        self.event_loop.add_reader(self.conn, self._handle_notify)
        self.disconnected = False

    def _handle_notify(self):
        try:
            self.conn.poll()
        except psycopg2.OperationalError:
            self.disconnected = True
            for queue in self.subscribers:
                queue.put_nowait("close")
            raise

        for notify in self.conn.notifies:
            payload = json.loads(notify.payload)
            for queue in self.subscribers:
                queue.put_nowait(payload)
        self.conn.notifies.clear()

    def get_listen_queue(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self.subscribers.append(queue)
        logger.info(f"Current active listeners: {len(self.subscribers)}")
        return queue

    def unsubscribe(self, queue):
        self.subscribers.remove(queue)
        logger.info(f"Current active listeners: {len(self.subscribers)}")


_pg_listener = PostgresListener(host=config.db_host,
                                dbname=config.db_name,
                                user=config.db_user,
                                password=config.db_password,
                                channel=config.notify_channel)


def get_postgres_listener():
    if _pg_listener.disconnected:
        logger.info("Postgres listener disconnected, reconnecting...")
        _pg_listener._connect()

    return _pg_listener
