from random import randrange
from time import sleep
from queue import Full as queueFullError
import ntplib
import matplotlib.pyplot as plt
from numpy.random import normal

# .py
from our_ntp import getLocalTime
# from streamer import STOP_TOKEN

GEM_RANGE   = 8
PRICE_RANGE = 5

PUT_TIMEOUT = 0

def rand_normal(mean, sd, lower_bound, upper_bound):
    return min( max( int( round( normal(mean, sd) ) ), 0), GEM_RANGE-1)

def gen_purchase(time_client=None):
    purchase = (
        rand_normal((GEM_RANGE-1)/2, GEM_RANGE/4, 0, GEM_RANGE),
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
    print("__________________\nTest ad_generator")

    # for i in range(10):
    #     print(gen_purchase())

    res = [rand_normal((GEM_RANGE-1)/2, GEM_RANGE/4, 0, GEM_RANGE) for i in range(10000)]
    dist = [res.count(i) for i in range(GEM_RANGE)]
    print(dist)

    fig, ax = plt.subplots(3)
    ax[0].hist(res)
    plt.savefig("plot.png")
