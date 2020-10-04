from random import randrange
from our_ntp import getLocalTime
import ntplib

USER_RANGE  = 1000 #idk
GEM_RANGE   = 5
PRICE_RANGE = 100

def gen_ad(time_client):
    return (
        randrange(USER_RANGE),
        randrange(GEM_RANGE),
        getLocalTime(time_client)
    )

def gen_purchase(time_client):
    return (
        randrange(GEM_RANGE),
        randrange(PRICE_RANGE),
        getLocalTime(time_client)
    )

def ad_generator(q, time_client, id, generation_rate):
    print("Start ad generator ", id)

    while True:
        start = getLocalTime(time_client)

        q.put(gen_ad(time_client))

        diff = getLocalTime(time_client) - start
        sleep(1/generation_rateS - diff)

def purchase_generator(q, time_client, id, generation_rate):
    print("Start purchase generator ", id)

    while True:
        start = getLocalTime(time_client)

        q.put(gen_purchase(time_client))

        diff = getLocalTime(time_client) - start
        sleep(1/generation_rateS - diff)

if __name__ == "__main__":
    print("Test purchase_generator")
    for i in range(10):
        print(gen_purchase(ntplib.NTPClient()))

    print("__________________\nTest ad_generator")

    for i in range(10):
        print(gen_ad(ntplib.NTPClient()))
