import os
import sys
import subprocess
import time
from deploy import deploy_all

MAX_WAIT_TIME = 920
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

def cancel_reservation(reservation_id):
    print("Cancelling reservation")
    os.system("preserve -c " + reservation_id)

	
if len(sys.argv) < 3:
    print("You must supply a number of worker nodes, and a data generation speed")
    exit()

# Read command line args
num_workers = int(sys.argv[1])

gen_rate = int(sys.argv[2])
print("Benchmarking generation rate ", gen_rate, " on ", num_workers, " workers")

# Reserve the nodes
os.system("preserve -# " + str(num_workers + 3) + " -t 00:15:00")
time.sleep(1)

# Get reservation info
reservation = get_reservation_info()
reservation_id = reservation[ID_IDX]
reservation_status = reservation[STATUS_IDX]
reserved_nodes = reservation[NODES_IDX:]

# Continuously check whether the nodes are available
cur_waiting_time=0
while reservation_status != "R" and reserved_nodes is not []:
    reservation = get_reservation_info()
    reservation_status = reservation[STATUS_IDX]
    reserved_nodes = reservation[NODES_IDX:]

    print("Reservation status: {}".format(reservation_status))
    time.sleep(POLLING_INTERVAL)
    cur_waiting_time += POLLING_INTERVAL

    # If it's taking too long, cancel the reservation
    if cur_waiting_time > MAX_WAIT_TIME:
        cancel_reservation(reservation_id)
        exit()

# If we've gotten here, the reservation is ready
print("Got reservation on nodes: ", reserved_nodes)
print("Deploying cluster.")

deploy_all(reserved_nodes, gen_rate, reservation_id)
