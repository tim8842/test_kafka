from aiokafka import AIOKafkaConsumer
import json


# Десериализатор для ключа
def key_deserializer(key_bytes):
    return key_bytes.decode("utf-8")


# Десериализатор для значения
def value_deserializer(value_bytes):
    return value_bytes.decode("utf-8")


async def consume():
    consumer = AIOKafkaConsumer(
        "broadcast-topic",
        bootstrap_servers="localhost:29092",
        group_id="group",
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
    )

    await consumer.start()

    async for msg in consumer:
        print(f"Key: {msg.key}, Value: {msg.value}")

    await consumer.stop()


import asyncio

asyncio.run(consume())
