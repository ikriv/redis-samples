import asyncio
import redis.asyncio

SLEEP_TIME_MS = 200
QUEUE_NAME = "myq"
STOP_MESSAGE = "stop"
NUMBER_OF_CONSUMERS = 3

r = redis.asyncio.Redis(host="localhost", port=6379, db=0, decode_responses=True)


async def producer():
    print(f"Producer")
    for i in range(50):
        await asyncio.sleep(SLEEP_TIME_MS / 1000)
        await r.rpush(QUEUE_NAME, f"message_{i+1}")

    for _ in range(NUMBER_OF_CONSUMERS):
        await r.rpush(QUEUE_NAME, STOP_MESSAGE)


async def consumer(id):
    print(f"Consumer {id}")
    while True:
        result = await r.blpop(QUEUE_NAME)
        if result is None:
            continue
        _, data = result
        print(f"Consumer {id} received: {data}")
        if data == STOP_MESSAGE:
            break


async def main():
    producer_task = asyncio.create_task(producer())
    consumers = [consumer(i + 1) for i in range(NUMBER_OF_CONSUMERS)]

    await asyncio.gather(*consumers)
    await producer_task

    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())
