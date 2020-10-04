from multiprocessing import Process, Queue
import socket
from our_ntp import getLocalTime
import ntplib
import generator

TEST = False

BUDGET = 1000000

GEN_RATE    = 2000 # Tuples/sec
N_PROCESSES = 2     # No. producing threads

HOST = "localhost"
PORT = 5555

SOCKET_TIMEOUT = 6000

results = [0, 0, 0, 0, 0]

def get_purchase_data(q):
    (gid, price, event_time) = q.get()
    results[gid] += price
    purchase = '{{ "gem":{}, "price":{}, "event_time":{} }}\n'.format(gid, price, event_time)
    print(purchase)
    return purchase

def stream_from_queue(q):
    print("Start Streamer")

    time_client = ntplib.NTPClient()

    if TEST:
        for i in range(BUDGET):
            data = get_purchase_data(q)

            print("TEST: got", data)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(SOCKET_TIMEOUT) # TODO determine/tweak
            s.bind((HOST, PORT))
            s.listen()
            print("waiting for connection")
            conn, addr = s.accept()

            with conn:
                print("Streamer connected by", addr)

                for i in range(BUDGET):
                    data = get_purchase_data(q)

                    conn.sendall(data.encode())

                    print('Sent tuple #', i)

def run():
    q = Queue()
    ad_generators = [ Process(target=generator.purchase_generator, args=(q, ntplib.NTPClient(), i,GEN_RATE,), daemon = True) for i in range(N_PROCESSES) ]

    for g in ad_generators:
        g.start()

    stream_from_queue(q)
    for i in range(5):
        print(i, ": ", results[i])

if __name__ == "__main__":
    run()
