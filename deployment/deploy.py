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


def storm_cli_config(zk_nimbus_node, worker_nodes, local):
    config = \
        " -c storm.zookeeper.servers=\"[\\\"" + zk_nimbus_node + IB_SUFFIX + "\\\"]\"" + \
        " -c nimbus.seeds=\"[\\\"" + zk_nimbus_node + IB_SUFFIX + "\\\"]\"" + \
        " -c storm.local.hostname=" + local + IB_SUFFIX + \
        " -c supervisor.supervisors=" + cli_worker_list(worker_nodes)
    return config


def cli_worker_list(worker_nodes):
    w_list = "\"[\\\""
    
    for i in worker_nodes:
        w_list += i + IB_SUFFIX
        if i is not worker_nodes[-1]:
            w_list += "\",\""
    w_list += "\\\"]\""

    return w_list


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
        " 'screen -d -m storm nimbus" + \
        storm_cli_config(node, worker_nodes, node) + "'"

    print("Deploying nimbus on " + node)
    os.system("ssh " + node + nimbus_start_command)


def deploy_workers(nodes, zk_nimbus_node):
    time.sleep(3)
    for i in nodes:
        # Create local storage folder
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-local'")
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-logs'")
        
        worker_start_command = \
            " 'screen -d -m storm supervisor" + \
            storm_cli_config(zk_nimbus_node, nodes, i) + "'"
 
        print("Deploying worker " + i)
        os.system("ssh " + i + worker_start_command)


def deploy_generator(node, gen_rate, reservation_id):
    # Start in screen to check output (only program that does not log to file)
    generator_start_command = \
	" 'screen -L -d -m python3 /home/ddps2016/DPS1/generator/benchmark_driver.py " + \
        str(BUDGET) + " " + str(gen_rate) + " " + str(NUM_GENERATORS) + "'"

    print("Deploying generator on " + node)
    os.system("ssh " + node + generator_start_command)


def deploy_mongo(node):
    print("Copying mongo files to node")
    os.system("ssh " + node + " 'mkdir -p /local/ddps2016/mongo_data'")
    os.system("ssh " + node + " 'rsync -r --delete /var/scratch/ddps2016/mongo_data/ /local/ddps2016/mongo_data'")

    mongo_start_command = \
	" 'screen -d -m numactl --interleave=all" + \
	" mongod --config /home/ddps2016/mongo/mongodb.conf &'"

    print("Deploying mongo server on " + node)
    os.system("ssh " + node + mongo_start_command);


def submit_topology(nimbus_node, generator_node, mongo_node, num_workers, worker_nodes, gen_rate):
    print("Waiting for the storm cluster to initialize...")
    time.sleep(15)
    
    submit_command = \
        "cd /home/ddps2016/DPS1/storm_bench; make submit" + \
        " ZK_ADDRESS=" + nimbus_node + IB_SUFFIX + \
        " NIMBUS_ADDRESS=" + nimbus_node + IB_SUFFIX + \
        " INPUT_ADRESS=" + generator_node + IB_SUFFIX + \
        " INPUT_PORT=5555" + \
        " MONGO_ADRESS=" + mongo_node + IB_SUFFIX + \
        " NUM_WORKERS=" + str(num_workers) + \
        " WORKER_LIST=" + cli_worker_list(worker_nodes) + \
        " GEN_RATE=" + str(gen_rate)

    print("Submitting topology to the cluster")
    os.system(submit_command)


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

    kill_command = \
    	"cd /home/ddps2016/DPS1/storm_bench; make kill" + \
    	" ZK_ADDRESS=" + zk_nimbus_node + IB_SUFFIX +\
    	" NIMBUS_ADDRESS=" + zk_nimbus_node + IB_SUFFIX
    os.system(kill_command)
    print("Spouts disabled. Waiting 30 seconds to process leftover tuples")
    time.sleep(35)

    # Export mongo data
    os.system(
        "mongoexport --host " + mongo_node + " -u storm -p test -d results -c aggregation " + \
        "-f \"GemID,latency,time\" --type=csv -o ~/result.csv"
    )

    # Cancel reservation
    os.system("preserve -c $(preserve -llist | grep ddps2016 | cut -f 1)")
    
    # Prompt to clean logs
    if autokill or input("Clean logs?") == "y":
        os.system("ssh " + zk_nimbus_node + " 'rm -r /local/ddps2016/storm-logs/*'")
        for i in worker_nodes:
            os.system("ssh " + i + " 'rm -r /local/ddps2016/storm-logs/*'")

        os.system("rm /home/ddps2016/zookeeper/logs/*")
        os.system("rm /home/ddps2016/mongo/log/*")

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

    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node, worker_nodes)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Deploy data input generator
    deploy_generator(generator_node, gen_rate, reservation_id)

    # Submit topology to the cluster
    submit_topology(zk_nimbus_node, generator_node, mongo_node, num_workers, worker_nodes, gen_rate)

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
        
