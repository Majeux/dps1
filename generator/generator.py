from random import randrange
from time import sleep
from our_ntp import getLocalTime
from queue import Full as queueFull
from benchmark_driver import STOP_TOKEN
import ntplib

USER_RANGE  = 1000 #idk
GEM_RANGE   = 5
PRICE_RANGE = 10

PUT_TIMEOUT = 0 #TODO probably very low?

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
        except queueFull as e:
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
        except queueFull as e:
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
