import asyncio
import json
from contextlib import asynccontextmanager
from typing import Annotated

import psycopg2.extensions
from fastapi import FastAPI, Depends
from loguru import logger
from sse_starlette import EventSourceResponse
from starlette.requests import Request

import db
from db import get_db
from postgres_listener import get_postgres_listener, PostgresListener


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def add_item(request: Request,
                   db: Annotated[psycopg2.extensions.connection, Depends(get_db)]):
    message = await request.body()
    message = message.decode()

    cursor = db.cursor()
    cursor.execute('''
    INSERT INTO item (message)
    VALUES (%s)
    ''', [message])
    db.commit()


@app.get("/updates")
def get_updates(req: Request,
                postgres_listener: Annotated[PostgresListener, Depends(get_postgres_listener)]):
    queue = postgres_listener.get_listen_queue()

    async def sse_wrapper():
        id = 0

        try:
            while (msg := await queue.get()) is not None and msg != "close":
                yield {
                    'id': id,
                    'event': 'message',
                    'data': json.dumps(msg)
                }
                id += 1

                if await req.is_disconnected():
                    break

        except asyncio.CancelledError as e:
            logger.info(f"Disconnected from client (via refresh/close) {req.client}")
            raise e
        finally:
            postgres_listener.unsubscribe(queue)

    return EventSourceResponse(sse_wrapper())
