import socket
from threading import Thread
from time import sleep
from datetime import datetime
import ntplib

TEST     = True            # Whether to run an additional consuming daemon
NTP_TIME = True

#limit data for debugging
INF_BUDGET  = False # Produce untill interrupted
BUDGET      = 10    # No. tuples produced

DATA_RATE           = 10 # Tuples/sec
PARALLEL_INSTANCES  = 1         # No. producing threads

HOST = "localhost"
PORT = 5555

def getLocalTime(client = None):
    if client != None:
        response = client.request('localhost')
        return response.tx_time

    return datetime.now().total_seconds()

def run_instance(thread_id):
    print("Start producer ", thread_id)

    time_client = ntplib.NTPClient()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()

        with conn:
            print(thread_id, ' connected by', addr)

            i = 0
            while True:
                start = getLocalTime(time_client)

                data = i
                conn.sendall(data.to_bytes(8, byteorder='big'))

                print(thread_id, " sent ", i)

                i = i + 1
                if not INF_BUDGET and i >= BUDGET:
                    break;

                diff = getLocalTime(time_client) - start

                sleep(1/DATA_RATE - diff)

def consume_test():
    print("Start consumer")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)
        s.connect((HOST, PORT))

        print('Consumer connected by', (HOST, PORT))

        while True:
            data = s.recv(1024)
            print("Received: ", int.from_bytes(data, 'big'))


if __name__ == "__main__":
    threads = list()

    for i in range(PARALLEL_INSTANCES):
        t = Thread(target=run_instance, args=(i,))
        threads.append(t)
        t.start()

    if TEST:
        test = Thread(target=consume_test, args=(), daemon=True)
        test.start()

    for t in threads:
        t.join()
