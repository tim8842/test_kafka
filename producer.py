from aiokafka import AIOKafkaProducer
import asyncio


async def main():
    producer = AIOKafkaProducer(bootstrap_servers="localhost:29092")
    await producer.start()
    try:
        for i in range(10):
            msg = f"Message {i}"
            await producer.send("broadcast-topic", msg.encode(), key="test".encode())
            await asyncio.sleep(2)
    except Exception as e:
        print(f"error {e}")
    finally:
        await producer.stop()


asyncio.run(main=main())
