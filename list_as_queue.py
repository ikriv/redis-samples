import redis
from multiprocessing import Process
import time

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
SLEEP_TIME_MS = 200
QUEUE_NAME = "myq"
STOP_MESSAGE = "stop"


def producer(id):
    print(f"Producer {id}")
    for i in range(50):
        time.sleep(SLEEP_TIME_MS / 1000)
        r.rpush(QUEUE_NAME, f"producer{id}_{i+1}")


def consumer():
    print(f"Consumer")
    while True:
        _, data = r.blpop(QUEUE_NAME)  # blocks
        print(f"Received: {data}")
        if data == STOP_MESSAGE:
            break


def start_process(target, args):
    p = Process(target=target, args=args)
    p.start()
    return p


def queue_stop():
    r.rpush(QUEUE_NAME, STOP_MESSAGE)


if __name__ == "__main__":
    producers = [start_process(producer, args=(i + 1,)) for i in range(3)]
    consumer_process = start_process(consumer, ())
    for producer_process in producers:
        producer_process.join()
    queue_stop()
    consumer_process.join()
