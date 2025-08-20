# kafka_producer.py
import asyncio
from aiokafka import AIOKafkaProducer
import json

producer: AIOKafkaProducer = None
TOPIC_NAME = "file-downloads"


async def start_producer():
    """Start Kafka producer on app startup."""
    global producer
    if producer is None:
        loop = asyncio.get_event_loop()
        producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            loop=loop
        )
        await producer.start()
        print("✅ Kafka producer started")


async def stop_producer():
    """Stop Kafka producer on app shutdown."""
    global producer
    if producer:
        await producer.stop()
        print("🛑 Kafka producer stopped")


async def send_message(key: str, value: dict):
    """Send a JSON message to Kafka."""
    global producer
    if not producer:
        raise RuntimeError("Producer not initialized. Did you forget startup event?")

    payload = json.dumps(value).encode("utf-8")
    await producer.send_and_wait(TOPIC_NAME, key=key.encode("utf-8"), value=payload)
    print(f"📩 Sent message → {TOPIC_NAME}: {value}")
