import os
import sys
import subprocess
import time

MAX_WAIT_TIME = 100
POLLING_INTERVAL = 2

if len(sys.argv) < 3):
	print("You must supply a number of nodes and a time to run")

# Read command line args
num_nodes = int(sys.argv[1])
run_time = sys.argv[2]
print("Running on " + sys.argv[1] + " nodes")

# Reserve the nodes
os.system("preserve -# " + str(num_nodes) + " -t " + run_time)
reservation_id = subprocess.check_output("preserve -llist | grep ddps2016 | awk '{ print $1 }'")
reserved_nodes = subprocess.check_output("preserve -llist | grep ddps2016 | awk '{ s = ""; for (i = 9; i <= NF; i++) s = s $i " "; print s }'").split()

# Continuously check whether the nodes are available
reservation_status="PD"
cur_waiting_time=0
while reservation_status != "R":
	reservation_status = subprocess.check_output("preserve -llist | grep ddps2016 | awk '{ print $7 }'")
	
	time.sleep(POLLING_INTERVAL)
	cur_waiting_time += POLLING_INTERVAL
	
	# If it's taking too long, cancel the reservation
	if cur_waiting_time > MAX_WAIT_TIME:
		os.system("preserve -c " + reservation_id)

# If we've gotten here, the reservation is ready

# Deploy the zookeeper nodes

# Deploy the nimbus node

# Deploy the worker nodes

# Deploy the generator
