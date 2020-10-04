from multiprocessing import Process, Queue
from socket import socket
from time import sleep
from datetime import datetime
import ntplib

BUDGET = 100

STREAM_RATE  = 10 # Tuples/sec
N_PROCESSES  = 1         # No. producing threads

HOST = "localhost"
PORT = 5555

def getLocalTime(client = None):
    if client != None:
        response = client.request('localhost')
        return response.tx_time

    return datetime.now().total_seconds()


def stream_from_queue(q):
    print("Start Streamer")

    time_client = ntplib.NTPClient()

    with socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10) # TODO determine/tweak
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()

        with conn:
            print("Streamer connected by", addr)

            for i in range(BUDGET):
                start = getLocalTime(time_client)

                (uid, gid, event_time) = q.get()
                data = '{ "user":{}, "gem":{}, "event_time":{} }'.format(uid, gid, event_time)
                conn.sendall(data.encode())

                print('Sent tuple #', i)

                diff = getLocalTime(time_client) - start

                sleep(1/STREAM_RATE - diff)

def consume_test():
    print("Start consumer")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)
        s.connect((HOST, PORT))

        print('Consumer connected by', (HOST, PORT))

        while True:
            data = s.recv(1024)
            print("Received: ", int.from_bytes(data, 'big'))

def run():
    q = Queue()
    ad_generators = [ Process(target=gen_ad, args=(q, ntplib.NTPClient(), i,), daemon = True) for i in range(N_PROCESSES) ]

    for g in ad_generators:
        g.start()

    stream_from_queue(q)

    for g in ad_generators:
        g.join()

if __name__ == "__main__":
