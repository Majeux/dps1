import random

def gen_userID():
    return 1

def gen_gemID():
    return 2

def gen_price():
    return 3

def gen_ad():
    return (gen_userID(), gen_gemID())

def gen_purchase():
    return (gen_userID(), gen_gemID(), gen_price())
