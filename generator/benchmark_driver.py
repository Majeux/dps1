from multiprocessing import Process, Queue
from math import ceil
from sys import argv
import socket
import ntplib

from our_ntp import getLocalTime #.py
import generator #.py

TEST = False                 #generate data without TCP connection
PRINT_CONN_STATUS = True    #print messages regarding status of socket connection
PRINT_CONFIRM_TUPLE = True #print tuples when they are being send

HOST = "localhost"
PORT = 5555

SOCKET_TIMEOUT = 6000

results = [0, 0, 0, 0, 0]

def get_purchase_data(q):
    (gid, price, event_time) = q.get()
    results[gid] += price
    purchase = '{{ "gem":{}, "price":{}, "event_time":{} }}\n'.format(gid, price, event_time)
    if PRINT_CONFIRM_TUPLE:
        print(purchase)
    return purchase

def stream_from_queue(q, generators, budget):
    if PRINT_CONN_STATUS:
        print("Start Streamer")

    time_client = ntplib.NTPClient()

    if TEST:
        for g in generators:
            g.start()

        for i in range(budget):
            data = get_purchase_data(q)

            if PRINT_CONFIRM_TUPLE:
                print("TEST: got", data)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(SOCKET_TIMEOUT) # TODO determine/tweak
            s.bind((HOST, PORT))
            s.listen()

            if PRINT_CONN_STATUS:
                print("waiting for connection")

            conn, addr = s.accept()

            with conn:
                if PRINT_CONN_STATUS:
                    print("Streamer connected by", addr)

                for g in generators:
                    g.start()

                for i in range(budget):
                    data = get_purchase_data(q)

                    conn.sendall(data.encode())

                    if PRINT_CONFIRM_TUPLE:
                        print('Sent tuple #', i)

def run(budget, rate, n_generators):
    q = Queue()

    ad_generators = [
        Process(
            target=generator.purchase_generator,
            args=(q, ntplib.NTPClient(), i, rate, ceil(budget/n_generators),),
            daemon = True
        ) for i in range(n_generators)
    ]

    stream_from_queue(q, ad_generators, budget)

    for i in range(5):
        print(i, ": ", results[i])

if __name__ == "__main__":
    try:
        budget       = int(argv[1]) if len(argv) > 1 else 1000000
        rate         = int(argv[2]) if len(argv) > 2 else 2000
        n_generators = int(argv[3]) if len(argv) > 3 else 4
    except ValueError:
        print("INVALID ARGUMENT TYPE!")
        print("Try `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`")
        quit()
    except:
        print("ERROR PARSING ARGUMENTS!")
        print("Try `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`")
        quit()

    run(budget, rate, n_generators)
