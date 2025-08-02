import asyncio
import redis.asyncio
import random
import time
import pprint

LOGIN_STREAM = "logins"
NUMBER_OF_LOGINS = 50
TIME_BETWEEN_LOGINS_MS = 100
NOTIFICATIONS_GROUP = "notifications"
ANALYTICS_GROUP = "analytics"
STOP_MESSAGE_TYPE = "stop"
LOGIN_MESSAEG_TYPE = "login"
ANALYTICS_HASH = "analytics"
NUM_NOTIFICATION_SERVICES = 2
NUM_ANALYTICS_SERVICES = 3

r = redis.asyncio.Redis(host="localhost", port=6379, db=0, decode_responses=True)

USERS = [
    "james",
    "mary",
    "john",
    "patricia",
    "robert",
    "jennifer",
    "michael",
    "linda",
    "william",
    "elizabeth",
    "david",
    "barbara",
    "richard",
    "susan",
    "joseph",
]


async def generate_logins():
    for _ in range(NUMBER_OF_LOGINS):
        user = random.choice(USERS)
        await r.xadd(
            LOGIN_STREAM,
            {"type": LOGIN_MESSAEG_TYPE, "user": user, "time": time.time()},
        )
        await asyncio.sleep(TIME_BETWEEN_LOGINS_MS / 1000)


async def notification_service(id):
    consumer_name = f"notif_{id}"
    while True:
        # response is an array of [(stream1, messages1), (stream2, messages2)]
        response = await r.xreadgroup(
            NOTIFICATIONS_GROUP, consumer_name, {LOGIN_STREAM: ">"}
        )
        if not response:
            continue

        for _stream, messages in response:
            for message_id, data in messages:
                type = data.get("type")
                if type == STOP_MESSAGE_TYPE:
                    print(f"{consumer_name}: STOP")
                    await r.xack(LOGIN_STREAM, NOTIFICATIONS_GROUP, message_id)
                    return
                if type == LOGIN_MESSAEG_TYPE:
                    print(
                        f"{consumer_name}: login notification for {data["user"]} at {data["time"]}"
                    )
                    await r.xack(LOGIN_STREAM, NOTIFICATIONS_GROUP, message_id)
                    continue
                print(f"{consumer_name}: Unknown message type: {type}")


async def add_login_to_analytics(user):
    if not user:
        return
    await r.hincrby(ANALYTICS_HASH, user, 1)


async def analytics_service(id):
    consumer_name = f"analytics_{id}"
    while True:
        # response is an array of [(stream1, messages1), (stream2, messages2)]
        response = await r.xreadgroup(
            ANALYTICS_GROUP, consumer_name, {LOGIN_STREAM: ">"}
        )
        if not response:
            continue

        for _stream, messages in response:
            for message_id, data in messages:
                type = data.get("type")
                if type == STOP_MESSAGE_TYPE:
                    print(f"{consumer_name}: STOP")
                    await r.xack(LOGIN_STREAM, ANALYTICS_GROUP, message_id)
                    return
                if type == LOGIN_MESSAEG_TYPE:
                    await add_login_to_analytics(data["user"])
                    await r.xack(LOGIN_STREAM, ANALYTICS_GROUP, message_id)
                    continue
                print(f"{consumer_name}: Unknown message type: {type}")


async def remove_login_stream():
    if await r.exists(LOGIN_STREAM):
        print(f"Stream {LOGIN_STREAM} exists")
        try:
            await r.delete(LOGIN_STREAM)
            print(f"Stream {LOGIN_STREAM} removed")
        except Exception as e:
            print(f"Error deleting {LOGIN_STREAM}: {e}")
    else:
        print(f"Stream {LOGIN_STREAM} does not exist")
    await r.delete(ANALYTICS_HASH)


async def print_analytics_data():
    data = await r.hgetall(ANALYTICS_HASH)
    print("Login analyitcs")
    pprint.pprint(data)


async def check_unacknowledged_messages():
    for group in [ANALYTICS_GROUP, NOTIFICATIONS_GROUP]:
        pending = await r.xpending_range(
            LOGIN_STREAM, group, min="-", max="+", count=100
        )
        if pending:
            print(f"There are {len(pending)} unacknowledged message for group {group}")
            for message_info in pending:
                message_id = message_info.get("message_id")
                try:
                    messages = await r.xrange(
                        LOGIN_STREAM, min=message_id, max=message_id
                    )
                    if messages:
                        print(f"{message_id}: {messages[0]}")
                    else:
                        print(f"{message_id}: UNKNOWN")
                except:
                    print(f"{message_id}: ERROR RETRIEVING MESSAGE")


async def main():
    print("REDIS STREAMS SAMPLE")
    await remove_login_stream()
    await r.xgroup_create(LOGIN_STREAM, NOTIFICATIONS_GROUP, mkstream=True)
    await r.xgroup_create(LOGIN_STREAM, ANALYTICS_GROUP, mkstream=True)
    print("Groups created")

    producer = asyncio.create_task(generate_logins())
    notifications = [
        asyncio.create_task(notification_service(id))
        for id in range(NUM_NOTIFICATION_SERVICES)
    ]
    analytics = [
        asyncio.create_task(analytics_service(id))
        for id in range(NUM_ANALYTICS_SERVICES)
    ]

    await producer

    for _ in range(max(NUM_NOTIFICATION_SERVICES, NUM_ANALYTICS_SERVICES)):
        await r.xadd(LOGIN_STREAM, {"type": STOP_MESSAGE_TYPE})

    await asyncio.gather(*notifications, *analytics)

    await print_analytics_data()
    await check_unacknowledged_messages()


if __name__ == "__main__":
    asyncio.run(main())
