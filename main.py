import asyncio
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Depends
from loguru import logger
from psycopg_pool.abc import CT
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
                   db: Annotated[CT, Depends(get_db)]):
    message = await request.body()
    message = message.decode()

    db.execute('''
    INSERT INTO item (message)
    VALUES (%s)
    ''', [message])
    db.commit()


@app.get("/updates")
async def get_updates(req: Request,
                postgres_listener: Annotated[
                    PostgresListener,
                    Depends(get_postgres_listener)
                ]):
    async def sse_wrapper():
        id = 0

        queue = postgres_listener.listen()

        try:
            while notify := await queue.get():
                msg = notify.payload

                if msg == "close":
                    break

                if await req.is_disconnected():
                    break

                yield {
                    # 'id': id,
                    # 'event': 'message',
                    'data': msg
                }
                id += 1

        except asyncio.CancelledError as e:
            logger.info(f"Disconnected from client (via refresh/close) {req.client}")
            raise e

        finally:
            postgres_listener.close(queue)

    return EventSourceResponse(sse_wrapper())
