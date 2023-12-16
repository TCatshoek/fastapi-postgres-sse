from typing import AsyncGenerator

import psycopg
from psycopg import Notify

import config
import db

_notify_conn = None

async def get_postgres_listener() -> AsyncGenerator[Notify, None]:
    global _notify_conn

    if _notify_conn is None:
        _notify_conn = await psycopg.AsyncConnection.connect(db.connection_string, autocommit=True)
        _notify_conn.execute(f"LISTEN {config.notify_channel};")

    return _notify_conn.notifies()
