import asyncio
import psycopg
from psycopg_pool.abc import ACT

import config
import db


class PostgresListener:
    def __init__(self, conn: ACT):
        self.conn = conn
        self.listen_task = None
        self.listeners = []

    async def start_listen_task(self):
        async def listen_task(conn: ACT) -> None:
            async for notify in self.conn.notifies():
                for queue in self.listeners:
                    await queue.put(notify)

        self.listen_task = asyncio.create_task(listen_task(self.conn))

    def listen(self):
        queue = asyncio.Queue()
        self.listeners.append(queue)
        return queue

    def close(self, queue):
        self.listeners.remove(queue)


_notify_conn = None
_postgres_listener = None


async def get_postgres_listener() -> PostgresListener:
    global _notify_conn
    global _postgres_listener

    if _notify_conn is None:
        _notify_conn = await psycopg.AsyncConnection.connect(db.connection_string, autocommit=True)
        await _notify_conn.execute(f"LISTEN {config.notify_channel};")

    if _postgres_listener is None:
        _postgres_listener = PostgresListener(_notify_conn)
        await _postgres_listener.start_listen_task()

    return _postgres_listener
