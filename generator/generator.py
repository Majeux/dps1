from scipy.stats import truncnorm
from random import randrange
from time import sleep
from queue import Full as queueFullError
from scipy.stats import truncnorm
import ntplib
import matplotlib.pyplot as plt

# .py
from our_ntp import getLocalTime
from benchmark_driver import STOP_TOKEN

GEM_RANGE   = 8
PRICE_RANGE = 5

PUT_TIMEOUT = 0

def rand_normal(mean, sd, low, mean):
        return truncnorm(
                    (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)

def gen_purchase(time_client=None):
    purchase = (
        rand_normal(GEM_RANGE/2, GEM_RANGE/4, 0, GEM_RANGE),
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

    for i in range(10):
        print(gen_purchase())

    # fig, ax = plt.subplots(3, sharex=True)
    # ax[0].hist(X1.rvs(10000), normed=True)
