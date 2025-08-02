import asyncio
import redis.asyncio
import random

MESSAGES_PER_PUBLISHER = 3
CHANNEL = "test news"
STOP_MESSAGE = "stop"

r = redis.asyncio.Redis(decode_responses=True)


# publish a message and sleep a random amount of time
async def publisher(id):
    print(f"Publisher {id}")
    for i in range(MESSAGES_PER_PUBLISHER):
        sleep_time = random.randint(1000, 5000)
        await asyncio.sleep(sleep_time / 1000)
        await r.publish(CHANNEL, f"Publisher {id} message {i+1}")


async def subscriber(id):
    async with r.pubsub() as listener:
        await listener.subscribe(CHANNEL)
        async for message in listener.listen():
            match message:
                case {"type": "message", "data": data}:
                    print(f"Subscriber {id} received {repr(data)}")
                    if data == STOP_MESSAGE:
                        break


async def main():
    publishers = [publisher(name) for name in ["John", "Paul", "George"]]
    subscribers = [asyncio.create_task(subscriber(name)) for name in ["A", "B"]]

    await asyncio.gather(*publishers)

    await r.publish(CHANNEL, "stop")
    await asyncio.gather(*subscribers)


if __name__ == "__main__":
    asyncio.run(main())
