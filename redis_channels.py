import redis
import time
import random
from multiprocessing import Process

MESSAGES_PER_PUBLISHER = 3
CHANNEL = "test news"
STOP_MESSAGE = "stop"

r = redis.Redis(decode_responses=True)


def start_process(target, args):
    p = Process(target=target, args=args)
    p.start()
    return p


# publish a message and sleep a random amount of time
def publisher(id):
    print(f"Publisher {id}")
    for i in range(MESSAGES_PER_PUBLISHER):
        sleep_time = random.randint(1000, 5000)
        time.sleep(sleep_time / 1000)
        r.publish(CHANNEL, f"Publisher {id} message {i+1}")


def subscriber(id):
    with r.pubsub() as listener:
        listener.subscribe(CHANNEL)
        for message in listener.listen():
            match message:
                case {"type": "message", "data": data}:
                    print(f"Subscriber {id} received {repr(data)}")
                    if data == STOP_MESSAGE:
                        break


if __name__ == "__main__":
    publishers = [
        start_process(publisher, args=(name,)) for name in ["John", "Paul", "George"]
    ]
    subscribers = [start_process(subscriber, args=(name,)) for name in ["A", "B"]]

    # wait for publishers to finish
    for publisher_process in publishers:
        publisher_process.join()

    r.publish(CHANNEL, "stop")
    for subscriber_process in subscribers:
        subscriber_process.join()
