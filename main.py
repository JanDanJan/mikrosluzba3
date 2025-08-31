import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from itertools import islice, cycle
from typing import Optional, List, Dict
from uuid import uuid4

from fastapi import FastAPI, Body
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from sqlalchemy import (
    MetaData, Table, Column, String, TIMESTAMP, text, Integer, Float
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import create_async_engine

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import random


# --------------------
# Settings
# --------------------
class Settings(BaseSettings):
    POSTGRES_DSN: str = "postgresql+asyncpg://walrus:walrus@walrus-db:5432/walrus"
    KAFKA_BOOTSTRAP: str = "localhost:9092"
    TOPIC_READY: str = "input-topic"
    TOPIC_DONE: str = "calc.completed"
    MOCK_N: int = 10
    LOG_LEVEL: str = "INFO"
    PORT: int = 8000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mock-ms")


# --------------------
# Database (SQLAlchemy Core, Async)
# --------------------
engine = create_async_engine(settings.POSTGRES_DSN, echo=False, pool_pre_ping=True)
metadata = MetaData()

# Minimal generic table (adjust to match WalrusSmartHub if desired)
electric_sensor = Table(
    "electric_sensor",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sensor_identification", String, nullable=False),
    Column("time_stamp", String),
    Column("description", String, nullable=False),
    Column("power", Float),
    Column("voltage", Float),
    Column("energy", Float),
    Column("curent", Float),
    Column("room_id", Integer),
)


# --------------------
# Kafka
# --------------------
producer: Optional[AIOKafkaProducer] = None
consumer_task: Optional[asyncio.Task] = None


# --------------------
# Mock payloads (cycle through these)
# --------------------
BASE_MOCKS: List[Dict] = [
    dict(
        id=100,
        sensor_identification="meter-01",
        description="Main corridor smart meter",
        power=120.5,     # Watts
        voltage=230.0,   # Volts
        energy=0.45,     # kWh
        curent=0.52,     # Amps (typo kept to match column name)
        room_id=1000
    ),
    dict(
        id=101,
        sensor_identification="meter-02",
        description="Kitchen meter",
        power=850.2,
        voltage=229.0,
        energy=3.10,
        curent=3.72,
        room_id=102
    ),
    dict(
        id=102,
        sensor_identification="meter-03",
        description="Server room meter",
        power=1540.0,
        voltage=231.0,
        energy=5.75,
        curent=6.66,
        room_id=1000
    ),
    dict(
        id=103,
        sensor_identification="meter-04",
        description="Office lighting circuit",
        power=420.0,
        voltage=230.0,
        energy=1.80,
        curent=1.83,
        room_id=1000
    ),
]


# --------------------
# FastAPI app
# --------------------
app = FastAPI(title="Mock Orchestrator Microservice")


@app.on_event("startup")
async def on_startup():
    # Create table if it doesn't exist
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        logger.info("Ensured 'events' table exists.")

    # Start Kafka producer
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info("Kafka producer started.")

    # Start Kafka consumer loop for completion topic
    loop = asyncio.get_event_loop()
    global consumer_task
    consumer_task = loop.create_task(consume_completion())
    logger.info("Kafka consumer task started (topic: %s).", settings.TOPIC_DONE)

def _fmt(v) -> str:
    if v is None:
        return ""
    # ensure no raw pipes in text fields to keep the format parseable
    return str(v).replace("|", r"\|")

def electric_row_to_pipe(row: dict, correlation_id: str) -> str:
    """
    Produce a pipe-separated event line from the electric_sensor row.
    Order: sensor_identification|time_stamp|description|power|voltage|energy|curent|room_id|correlation_id
    """
    return "|".join([
        str(random.randint(1,10)),
        "electric_sensor",
        _fmt(row.get("id")),
    ])


@app.on_event("shutdown")
async def on_shutdown():
    # Stop consumer task
    global consumer_task
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    # Stop producer
    global producer
    if producer:
        await producer.stop()

    # Close DB engine
    await engine.dispose()


async def consume_completion():
    consumer = AIOKafkaConsumer(
        settings.TOPIC_DONE,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id="ms2-calculator",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # auto_offset_reset can be "earliest" if you want to read history
        auto_offset_reset="latest",
    )
    await consumer.start()
    logger.info("Kafka consumer connected (topic: %s)", settings.TOPIC_DONE)
    try:
        async for msg in consumer:
            payload = msg.value
            logger.info("Received completion: %s", payload)
    finally:
        await consumer.stop()


class RunRequest(BaseModel):
    n: Optional[int] = None
    description: Optional[str] = None  # override description for this batch if desired


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/run")
async def run_insert_and_publish(body: RunRequest = Body(default=RunRequest())):
    """
    Insert n mock rows into electric_sensor and publish a Kafka event for each insert.
    """
    n = body.n or settings.MOCK_N
    now_iso = datetime.now(timezone.utc).isoformat()

    # build n records by cycling base mocks; set time_stamp per-record to current ISO
    src = list(islice(cycle(BASE_MOCKS), n))

    inserted_count = 0
    published_count = 0

    async with engine.begin() as conn:
        for base in src:
            # per-row values
            row = dict(base)
            row["time_stamp"] = now_iso  # string timestamp per your schema
            if body.description:
                row["description"] = body.description  # allow override for testing batches

            # insert row
            await conn.execute(electric_sensor.insert().values(**row))
            inserted_count += 1

            # build pipe-separated event from inserted row
            correlation_id = str(uuid4())
            event_str = electric_row_to_pipe(row, correlation_id)

            # publish as bytes (since producer is byte-oriented)
            await producer.send_and_wait(settings.TOPIC_READY, event_str.encode("utf-8"))
            published_count += 1

            logger.info(
                "Inserted row sensor=%s; published pipe event to %s",
                row["sensor_identification"], settings.TOPIC_READY
            )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=settings.PORT, reload=False)