import os
import time
import sched
import multiprocessing
import ctypes

dead = multiprocessing.Value(ctypes.c_bool, False)
lock = multiprocessing.Lock()

BUDGET = 4000000
NUM_GENERATORS = 16
IB_SUFFIX = ".ib.cluster"


# Deploys the zookeeper server, and a storm nimbus on the same node
def deploy_zk_nimbus(node, worker_nodes):
    # Start the zookeeper server
    zk_start_command = " 'zkServer.sh start'"
    os.system("ssh " + node + zk_start_command)
    time.sleep(2)    

    # Create local storage folder
    os.system("ssh " + node + " 'mkdir -p /local/ddps2016/storm-local'")
    os.system("ssh " + node + " 'mkdir -p /local/ddps2016/storm-logs'")

    #Start the nimbus
    nimbus_start_command = \
        " 'screen -d -m storm nimbus -c storm.local.hostname=" + node + IB_SUFFIX + "'"

    print("Deploying nimbus on " + node)
    os.system("ssh " + node + nimbus_start_command)


# Deploys the storm supervisors
def deploy_workers(nodes, zk_nimbus_node):
    for i in nodes:
        # Create local storage folder
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-local'")
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-logs'")
        
        worker_start_command = \
            " 'screen -d -m storm supervisor -c storm.local.hostname=" + i + IB_SUFFIX + "'"
 
        print("Deploying supervisor on node " + i)
        os.system("ssh " + i + worker_start_command)


# Deploys the custom data generator
def deploy_generator(node, gen_rate, reservation_id):
    # Start in screen to check output (only program that does not log to file)
    generator_start_command = \
	" 'screen -L -d -m python3 /home/ddps2016/DPS1/generator/benchmark_driver.py " + \
        str(BUDGET) + " " + str(gen_rate) + " " + str(NUM_GENERATORS) + "'"

    print("Deploying generator on " + node)
    os.system("ssh " + node + generator_start_command)


# Deploys the mongo database server
def deploy_mongo(node):
    print("Copying mongo files to node")
    os.system("ssh " + node + " 'mkdir -p /local/ddps2016/mongo_data'")
    os.system("ssh " + node + " 'rsync -r --delete /var/scratch/ddps2016/mongo_data/ /local/ddps2016/mongo_data'")

    mongo_start_command = \
	" 'screen -d -m numactl --interleave=all" + \
	" mongod --config /home/ddps2016/mongo/mongodb.conf &'"

    print("Deploying mongo server on " + node)
    os.system("ssh " + node + mongo_start_command);


# Submits topology to the cluster
def submit_topology(nimbus_node, generator_node, mongo_node, num_workers, worker_nodes, gen_rate):
    print("Waiting for the storm cluster to initialize...")
    time.sleep(12)
    
    submit_command = \
        "cd /home/ddps2016/DPS1/storm_bench; make submit" + \
        " INPUT_ADRESS=" + generator_node + IB_SUFFIX + \
        " INPUT_PORT=5555" + \
        " MONGO_ADRESS=" + mongo_node + IB_SUFFIX + \
        " NUM_WORKERS=" + str(num_workers) + \
        " GEN_RATE=" + str(gen_rate)

    print("Submitting topology to the cluster")
    os.system(submit_command)


# Helper for gen_config_file
def worker_list(worker_nodes):
    w_list = ""
    for i in worker_nodes:
        w_list += i + IB_SUFFIX
        if i is not worker_nodes[-1]:
            w_list += ","
    return w_list

# Needs to all be set in config file, because the cli settings don't work well
def gen_config_file(zk_nimbus_node, worker_nodes):
    os.system(
        "cat ~/storm/conf/storm-defaults.yaml | sed \'"
        "s/NIM_SEED/" + zk_nimbus_node + IB_SUFFIX + "/g; " + \
        "s/ZOO_SERVER/" + zk_nimbus_node + IB_SUFFIX + "/g; " + \
        "s/SUPERVISORS/" + worker_list(worker_nodes) + "/g" + \
        "' > ~/storm/conf/storm.yaml")


# Kills the cluster in a contolled fashion
def kill_cluster(zk_nimbus_node, mongo_node, worker_nodes, autokill):
    global dead
    global lock

    lock.acquire()
    if dead.value:
        lock.release()
        return
    dead.value = True 
    lock.release()   

    print("Killing cluster{}.".format(" automatically" if autokill else ""))

    # Kill the topology
    os.system("cd /home/ddps2016/DPS1/storm_bench; make kill")
    print("Spouts disabled. Waiting 15 seconds to process leftover tuples")
    time.sleep(17)

    # Reset zookeeper storm files
    os.system("zkCli.sh -server " + zk_nimbus_node + ":2186 deleteall /storm")

    # Export mongo data
    os.system(
        "mongoexport --host " + mongo_node + " -u storm -p test -d results -c aggregation " + \
        "-f \"GemID,latency,time\" --type=csv -o ~/result.csv"
    )

    # Clean storm local storage
    os.system("ssh " + zk_nimbus_node + " 'rm -rf /local/ddps2016/storm-local/*'")
    for i in worker_nodes:
        os.system("ssh " + i + " 'rm -rf /local/ddps2016/storm-local/*'")

    # Clean mongo data
    os.system("ssh " + mongo_node + " 'rm -rf /local/ddps2016/mongo_data/*'")

    # Prompt to clean logs
    if autokill or input("Clean logs?\n") == "y":
        os.system("ssh " + zk_nimbus_node + " 'rm -r /local/ddps2016/storm-logs/*'")
        for i in worker_nodes:
            os.system("ssh " + i + " 'rm -rf /local/ddps2016/storm-logs/*'")

        os.system("rm /home/ddps2016/zookeeper/logs/*")
        os.system("rm /home/ddps2016/mongo/log/*")
    
    # Cancel reservation
    os.system("preserve -c $(preserve -llist | grep ddps2016 | cut -f 1)")

    if autokill:
        print("Automatic shutdown successful, press enter continue")


def auto_shutdown(zk_nimbus_node, mongo_node, worker_nodes):
    s = sched.scheduler(time.time, time.sleep)
    s.enter(14*60, 1, kill_cluster, argument=(zk_nimbus_node, mongo_node, worker_nodes, True,))
    s.run(True)    


def deploy_all(available_nodes, gen_rate, reservation_id):
    global dead
    global lock
    assert len(available_nodes) > 3
    
    # Assign nodes
    zk_nimbus_node = available_nodes[0]
    generator_node = available_nodes[1]
    mongo_node = available_nodes[2]
    worker_nodes = available_nodes[3:]
    num_workers = len(worker_nodes)

    # Set up a timer to close everything neatly after 15 minutes
    p = multiprocessing.Process(target=auto_shutdown, args=(zk_nimbus_node, mongo_node, worker_nodes,))
    p.start()
    
    # Deploy mongo server
    deploy_mongo(mongo_node)

    # Set nimbus hosts in config file, because it doesnt work for some reason
    gen_config_file(zk_nimbus_node, worker_nodes)
    
    # Deploy data input generator
    deploy_generator(generator_node, gen_rate, reservation_id)
    
    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node, worker_nodes)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Submit topology to the cluster
    submit_topology(zk_nimbus_node, generator_node, mongo_node, num_workers, worker_nodes, gen_rate)

    # Wait for kill command, or kill automatically if out of time
    while True:
        _in = input("Type \"k\" to kill the cluster\n")

        lock.acquire()
        if dead.value:
            p.join()
            lock.release()
            break
        elif _in == "k":
            p.terminate()
            lock.release()
            kill_cluster(zk_nimbus_node, mongo_node, worker_nodes, False)
            break
        
