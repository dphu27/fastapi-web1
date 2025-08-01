from aiokafka import AIOKafkaConsumer
import asyncio
import json

class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers="localhost:9092"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.task = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="exercise-consumer-group"
        )
        await self.consumer.start()
        self.task = asyncio.create_task(self.consume())

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.task:
            self.task.cancel()

    async def consume(self):
        try:
            async for msg in self.consumer:
                print(f"[Kafka] Nhận: {msg.value}")
        except Exception as e:
            print(f"Lỗi Kafka Consumer: {e}")
