#!/bin/python3
from multiprocessing import Process, Queue, Lock, Value
import ctypes
from queue import Empty as queueEmptyError
from time import sleep
from time import time
from math import ceil
from sys import argv
import socket
import ntplib

#.py
import generator

# TODO kijk naar  timeouts thresholds
# TODO log queue sizes op interval

STOP_TOKEN = "_STOP_"

class Streamer:
    # statics
    TEST = False                #generate data without TCP connection
    PRINT_CONN_STATUS = True    #print messages regarding status of socket connection
    PRINT_CONFIRM_TUPLE = False #print tuples when they are being send
    PRINT_QUEUE_SIZES = True    #print the sizes of the queue during the run

    SOCKET_TIMEOUT = 6000
    HOST = "0.0.0.0"
    PORT = 5555

    QUEUE_BUFFER_SECS = 5 # the maximum size of the queue expressed in seconds of generation
    GET_TIMEOUT = 10

    QUEUE_LOG_INTERVAL = 0.5 # time in seconds between queuesize logs

    # Object variables
    #   q: Queue        --
    #   error_q: Queue  -- Communicates error from child to Streamer
    #   generators: Process(generator.purchase_generator)
    #   budget: int
    #   generation_rate: int
    #   results: [int]
    #   q_size_log: [int]

    def __init__(self, budget, rate, n_generators, ntp_address):
        self.q = Queue(rate * self.QUEUE_BUFFER_SECS)
        print("Queue maxsize: {}".format(self.q._maxsize))
        self.error_q = Queue()
        self.budget = budget
        self.results = [0]*generator.GEM_RANGE
        self.q_size_log = []
        self.done_sending = Value(ctypes.c_bool, False)

        sub_rate = rate/n_generators
        sub_budget = ceil(budget/n_generators) # overestimate with at most n_generators

        if ntp_address == None:
            ntp_clients = [None] * n_generators
        else:
            ntp_clients = [ (ntplib.NTPClient(), ntp_address) ]  * n_generators

        self.qsize_log_thread = Process(target=self.log_qsizes, args=())  

        self.generators = [
            Process(target=generator.purchase_generator,
            args=(self.q, self.error_q, (ntp_clients[i]), i, sub_rate, sub_budget,),
            daemon = True)
        for i in range(n_generators) ]

        
    # end -- def __init__

    def log_qsizes(self):
        if not self.PRINT_QUEUE_SIZES:
            return

        start = time()
        while not self.done_sending.value:
            print("|Q|@ ", time()-start, ":", self.q.qsize())
            sleep(self.QUEUE_LOG_INTERVAL)
        
        print("Time taken: {}".format(time()-start))

    # end -- def log_qsizes

    def run(self):
        try:
            if self.TEST:
                self.stream_test()
            else:
                self.stream_from_queue()
        except:
            raise
        finally:
            if self.PRINT_CONFIRM_TUPLE:
                for i, r in enumerate(self.results):
                    print(i, ": ", r)

    # end -- def run

    def consume_loop(self, consume_f, *args):
        for g in self.generators:
            g.start()

        for i in range(self.budget):
            data = self.get_purchase_data()

            if data == STOP_TOKEN:
                self.done_sending.value = True
                raise RuntimeError("Aborting Streamer, exception raised by generator")

            consume_f(data, i, *args)
    
    # end -- def consume_loop

    def stream_test(self):
        def print_to_terminal(data, i):
            if self.PRINT_CONFIRM_TUPLE:
                print("TEST{}: got".format(i), data)

        self.consume_loop(print_to_terminal, ())
    # end -- def stream_test

    def stream_from_queue(self):
        def send(data, i, c):
            c.sendall(data.encode())

            if self.PRINT_CONFIRM_TUPLE:
                print('Sent tuple #', i)

        if self.PRINT_CONN_STATUS:
            print("Start Streamer")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(self.SOCKET_TIMEOUT) # TODO determine/tweak
            s.bind((self.HOST, self.PORT))
            s.listen(0)

            if self.PRINT_CONN_STATUS:
                print("waiting for connection")

            conn, addr = s.accept()

            with conn:
                if self.PRINT_CONN_STATUS:
                    print("Streamer connected by", addr)
                
                self.qsize_log_thread.start()
                self.consume_loop(send, conn)
                
                print("All tuples sent, waiting for cluster in recv")
                self.done_sending.value = True
                conn.recv(1)

    # end -- def stream_from_queue

    def get_purchase_data(self):
        try: #check for errors from generators
            return self.error_q.get_nowait()
        except Exception:
            pass # There was no error raised

        try:
            (gid, price, event_time) = self.q.get(timeout=self.GET_TIMEOUT)
        except queueEmptyError as e:
            raise RuntimeError('Streamer timed out getting from queue') from e

        purchase = '{{ "gem":{}, "price":{}, "event_time":{} }}\n'.format(gid, price, event_time)

        if self.PRINT_CONFIRM_TUPLE:
            self.results[gid] += price
            print(purchase)

        return purchase
    # end -- def get_purchase_data

def arg_to_int(arg, name):
    try:
        return int(arg)
    except ValueError as e:
        raise RuntimeError('\n\t commandline argument of invalid type.\n\t`{}` must be of type `int`\n\tUse: `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`'.format(name)) from e
    except Exception:
        raise

if __name__ == "__main__":
    if len(argv) < 4:
        raise ValueError('\n\tToo few arguments.\n\tUse: `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`')

    budget       = arg_to_int(argv[1], "budget")
    rate         = arg_to_int(argv[2], "generation_rate")
    n_generators = arg_to_int(argv[3], "n_generators")
    ntp_address  = argv[4] if len(argv) > 4 else None

    driver = Streamer(budget, rate, n_generators, ntp_address)
    driver.run()
