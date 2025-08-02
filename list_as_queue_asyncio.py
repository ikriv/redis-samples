import asyncio
import redis.asyncio

SLEEP_TIME_MS = 200
QUEUE_NAME = "myq"
STOP_MESSAGE = "stop"

r = redis.asyncio.Redis(host="localhost", port=6379, db=0, decode_responses=True)


async def producer(id):
    print(f"Producer {id}")
    for i in range(50):
        await asyncio.sleep(SLEEP_TIME_MS / 1000)
        await r.rpush(QUEUE_NAME, f"producer{id}_{i+1}")


async def consumer():
    print("Consumer")
    while True:
        result = await r.blpop(QUEUE_NAME)
        if result is None:
            continue
        _, data = result
        print(f"Received: {data}")
        if data == STOP_MESSAGE:
            break


async def main():
    producers = [producer(i + 1) for i in range(3)]
    consumer_task = asyncio.create_task(consumer())
    await asyncio.gather(*producers)
    await r.rpush(QUEUE_NAME, STOP_MESSAGE)
    await consumer_task


if __name__ == "__main__":
    asyncio.run(main())
