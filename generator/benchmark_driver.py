from multiprocessing import Process, Queue
from queue import Empty as queueEmpty
from math import ceil
from sys import argv
import socket
import ntplib

import generator #.py

class BenchmarkDriver:
    # statics
    TEST = False                 #generate data without TCP connection
    PRINT_CONN_STATUS = True    #print messages regarding status of socket connection
    PRINT_CONFIRM_TUPLE = True #print tuples when they are being send

    SOCKET_TIMEOUT = 6000
    HOST = "localhost"
    PORT = 5555

    QUEUE_MAX = 1000 # TODO configure

    # Object variables
    #   q: Queue
    #   generators: Process(generator.purchase_generator)
    #   budget: int
    #   generation_rate: int
    #   results: dictionary
    #   q_size_data: [int]

    def __init__(self, budget, rate, n_generators, ntp_address):
        self.q = Queue(self.QUEUE_MAX)
        self.budget = budget
        self.results = [0]*generator.GEM_RANGE
        self.q_size_data = []

        sub_rate = rate/n_generators
        sub_budget = ceil(budget/n_generators) # overestimate with at most n_generators

        if ntp_address == None:
            ntp_clients = [None] * n_generators
        else:
            ntp_clients = [ (ntplib.NTPClient(), ntp_address) ]  * n_generators

        self.generators = [
            Process(target=generator.purchase_generator, args=(self.q, (ntp_clients[i]), i, sub_rate, sub_budget,), daemon = True)
        for i in range(n_generators) ]

    def get_purchase_data(self):
        try:
            (gid, price, event_time) = self.q.get(timeout=0.1)
        except queueEmpty as e:
            raise RuntimeError('Streamer timed out getting from queue') from e

        purchase = '{{ "gem":{}, "price":{}, "event_time":{} }}\n'.format(gid, price, event_time)

        if self.PRINT_CONFIRM_TUPLE:
            self.results[gid] += price
            print(purchase)

        return purchase

    def run(self):
        if self.TEST:
            self.stream_test()
        else:
            self.stream_from_queue()

            if self.PRINT_CONFIRM_TUPLE:
                for i, r in enumerate(self.results):
                    print(i, ": ", r)


    def stream_test(self):
        for g in self.generators:
            g.start()

        for i in range(self.budget):
            data = self.get_purchase_data()

            if self.PRINT_CONFIRM_TUPLE:
                print("TEST: got", data)

    def stream_from_queue(self):
        if self.PRINT_CONN_STATUS:
            print("Start Streamer")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(self.SOCKET_TIMEOUT) # TODO determine/tweak
            s.bind((self.HOST, self.PORT))
            s.listen()

            if self.PRINT_CONN_STATUS:
                print("waiting for connection")

            conn, addr = s.accept()

            with conn:
                if self.PRINT_CONN_STATUS:
                    print("Streamer connected by", addr)

                for g in self.generators:
                    g.start()

                for i in range(selfbudget):
                    data = self.get_purchase_data()

                    conn.sendall(data.encode())

                    if self.PRINT_CONFIRM_TUPLE:
                        print('Sent tuple #', i)


if __name__ == "__main__":
    try:
        budget       = int(argv[1]) if len(argv) > 1 else 1000000
        rate         = int(argv[2]) if len(argv) > 2 else 2000
        n_generators = int(argv[3]) if len(argv) > 3 else 4
        ntp_address  = argv[4]      if len(argv) > 4 else None
    except ValueError:
        print("INVALID ARGUMENT TYPE!")
        print("Try `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint] [ntp_address: string]`")
        quit()
    except:
        print("ERROR PARSING ARGUMENTS!")
        print("Try `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint] [ntp_address: string]`")
        quit()

    driver = BenchmarkDriver(budget, rate, n_generators, ntp_address)
    driver.run()
