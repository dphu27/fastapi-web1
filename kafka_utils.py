import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "exercise_topic"

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None

# Khởi động producer
async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    print("Kafka Producer started")

# Dừng producer
async def stop_kafka_producer():
    global producer
    if producer:
        await producer.stop()
        print("Kafka Producer stopped")
        producer = None

# Gửi message
async def send_to_kafka(message: str):
    if not producer:
        raise Exception("Producer chưa được khởi động")
    await producer.send_and_wait(KAFKA_TOPIC, message.encode("utf-8"))
    print(f" Sent: {message}")

# Consumer theo doc
async def start_kafka_consumer(callback):
    global consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="exercise-group"
    )
    await consumer.start()
    print(" Kafka Consumer started")

    async def consume():
        try:
            async for msg in consumer:
                print(f" Received: {msg.value.decode()}")
                await callback(msg.value.decode())
        finally:
            await consumer.stop()

    asyncio.create_task(consume())
