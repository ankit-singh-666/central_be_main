import asyncio
from aiokafka import AIOKafkaConsumer

KAFKA_TOPIC = "file-downloads"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="file-processor-group",  # Consumers in same group share work
        auto_offset_reset="earliest",    # Start from earliest messages if no offset
    )

    # Start consumer
    await consumer.start()
    try:
        print(f"✅ Consumer started. Listening on topic: {KAFKA_TOPIC}")
        async for msg in consumer:
            file_id = msg.value.decode("utf-8")
            print(f"📂 Received File ID: {file_id}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
