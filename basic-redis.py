import sys
import redis
import time
import random


def print_versions():
    (major, minor, micro, *_) = sys.version_info
    print(f"Python {major}.{minor}.{micro}")
    print(f"Redis version: {redis.__version__}")


# Connect to Redis (default localhost:6379)
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def handle_simple_get_set_with_expiration():
    # we repeat a loop every 100ms, setting a new value every 500ms
    #
    new_value = random.randint(10, 99)
    ttl_ms = 200
    SLEEP_TIME_MS = 100
    TOTAL_ITERATIONS = 50
    ITERATIONS_PER_VALUE_CHANGE = 5  # iterations
    for iteration in range(TOTAL_ITERATIONS):
        if iteration % ITERATIONS_PER_VALUE_CHANGE == 0:
            # update value, set expiration
            current_value = r.set("secret_data", new_value, get=True, px=ttl_ms)
            ttl_ms += 100
            print(
                f"Setting secret_data to {new_value}, expires in {ttl_ms}ms, old value is {repr(current_value)}"
            )
            new_value += 1
        else:
            current_value = r.get("secret_data")
            print(repr(current_value))
        time.sleep(SLEEP_TIME_MS / 1000)


def handle_incr_decr():
    # we select a random value and then increment or decrement it randomly
    SLEEP_TIME_MS = 100
    value = random.randint(10, 100)
    print(f"Setting data to {value}")
    r.set("data", value)
    for _ in range(50):
        incr = random.randint(0, 1) > 0
        if incr:
            new_value = r.incr("data")
            print(f"+ to {repr(new_value)}")
        else:
            new_value = r.decr("data")
            print(f"- to {repr(new_value)}")
        time.sleep(SLEEP_TIME_MS / 1000)


def handle_hash():
    delete_result = r.delete("country:42")
    print(f"delete returns {delete_result}")

    delete_result = r.delete("country:42")
    print(f"delete returns {delete_result}")

    data = {
        "name": "France",
        "language": "French",
    }
    hset_result = r.hset("country:42", mapping=data)
    print(f"hset returned {repr(hset_result)}")

    name = r.hget("country:42", "name")
    print(f"country name is {repr(name)}")

    r.hset("country:42", "name", "Belgium")
    name = r.hget("country:42", "name")
    print(f"country name is {repr(name)}")

    data = r.hgetall("country:42")
    print(f"country information: {data}")

    r.hset("country:42", items=["language", "Flemish", "capital", "Brussels"])
    data = r.hgetall("country:42")
    print(f"country information: {data}")


def handle_list():
    r.delete("mylist")
    push_retval = r.lpush("mylist", "one", "two", "three")
    print(f"lpush returns {repr(push_retval)}")
    # can't use r.get() on lists
    all_list = r.lrange("mylist", 0, -1)
    print(f"Entire list is now {repr(all_list)}")


print_versions()
# handle_simple_get_set_with_expiration()
# handle_incr_decr()
handle_list()
