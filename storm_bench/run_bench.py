import os
import sys
import subprocess
import time
from deploy import deploy_all

MAX_WAIT_TIME = 100
POLLING_INTERVAL = 2

# COLUMN INDEXES OF RESERVATION INFO
ID_IDX=0
NODES_IDX=8
STATUS_IDX=6

def get_reservation_info():
    result = subprocess.check_output("preserve -llist | grep ddps2016", shell=True).decode("utf-8")
    if result.count("\n") > 1:
        raise Exception("Multiple reservations found")
    return result.split()


if len(sys.argv) < 2:
        print("You must supply a number of worker nodes")
        exit()

# Read command line args
num_workers = int(sys.argv[1])
print("Benchmarking with " + sys.argv[1] + " workers")

# Reserve the nodes
os.system("preserve -# " + str(num_workers + 2) + " -t 00:15:00")
time.sleep(1)
try:
    reservation = get_reservation_info()
except Exception as e:
    print(e)
    exit()

reservation_id = reservation[ID_IDX]
reserved_nodes = reservation[NODES_IDX:]

# Continuously check whether the nodes are available
reservation_status=get_reservation_info()[STATUS_IDX]
cur_waiting_time=0
while reservation_status != "R":
    reservation_status = get_reservation_info()[STATUS_IDX]

    print(reservation_status)
    time.sleep(POLLING_INTERVAL)
    cur_waiting_time += POLLING_INTERVAL

    # If it's taking too long, cancel the reservation
    if cur_waiting_time > MAX_WAIT_TIME:
        os.system("preserve -c " + reservation_id)

# If we've gotten here, the reservation is ready
generator = deploy_all(reserved_nodes)
