import random
import socket
import threading
import time

TEST = True

#limit data for debugging
INF_BUDGET = False
BUDGET = 10

DATA_RATE = 100000000
PARALLEL_INSTANCES = 2

HOST = "localhost"
PORT = 8080

def run_instance(thread_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()

        with conn:
            print(thread_id, ' connected by', addr)

            i = 0
            while True:
                data = i.to_bytes(8, byteorder='big')
                conn.sendall(data)

                i = i + 1
                if not INF_BUDGET and i >= BUDGET:
                    break;

                time.sleep(1)

def consume_test():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        while True:
            data = s.recv(1024)
            print("Received: ", int.from_bytes(data, 'big'))


if __name__ == "__main__":
    threads = list()

    for i in range(PARALLEL_INSTANCES):
        t = threading.Thread(target=run_instance, args=(i,))
        threads.append(t)
        t.start()

    if TEST:
        test = threading.Thread(target=consume_test, args=(), daemon=True)

    for t in threads:
        t.join()
