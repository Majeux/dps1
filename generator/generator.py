from random import randrange
from time import sleep
from queue import Full as queueFullError
import ntplib

# .py
from our_ntp import getLocalTime
from benchmark_driver import STOP_TOKEN

USER_RANGE  = 1000 #idk
GEM_RANGE   = 8
PRICE_RANGE = 5

PUT_TIMEOUT = 0

def gen_ad(time_client):
    return (
        randrange(USER_RANGE),
        randrange(GEM_RANGE),
        getLocalTime(time_client)
    )

def gen_purchase(time_client):
    purchase = (
        randrange(GEM_RANGE),
        randrange(PRICE_RANGE),
        getLocalTime(time_client)
    )
    return purchase


def ad_generator(q, error, time_client, id, rate, budget):
    print("Start ad generator ", id)

    for i in range(budget):
        start = getLocalTime(time_client)

        try:
            q.put(gen_ad(time_client), PUT_TIMEOUT)
        except queueFullError as e:
            error.put(STOP_TOKEN)
            raise RuntimeError("\n\tGenerator-{}reached Queue threshold\n".format(id)) from e

        diff = getLocalTime(time_client) - start
        sleep(max(0, 1/rate - diff))

def purchase_generator(q, error, time_client, id, rate, budget):
    print("Start purchase generator ", id)

    for i in range(budget):
        start = getLocalTime(time_client)

        try:
            q.put(gen_purchase(time_client), PUT_TIMEOUT)
        except queueFullError as e:
            error.put(STOP_TOKEN)
            raise RuntimeError("\n\tGenerator-{} reached Queue threshold\n".format(id)) from e


        diff = getLocalTime(time_client) - start
        sleep(max(0, 1/rate - diff))

if __name__ == "__main__":
    print("Test purchase_generator")
    for i in range(10):
        print(gen_purchase(ntplib.NTPClient()))

    print("__________________\nTest ad_generator")

    for i in range(10):
        print(gen_ad(ntplib.NTPClient()))
