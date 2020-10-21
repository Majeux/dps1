import os
import time

BUDGET = 100000
NUM_GENERATORS = 16

def storm_cli_config(zk_nimbus_node, worker_nodes, local):
    config = \
        " -c storm.zookeeper.servers=\"[\\\"" + zk_nimbus_node + "\\\"]\"" + \
        " -c nimbus.seeds=\"[\\\"" + zk_nimbus_node + "\\\"]\"" + \
        " -c storm.local.hostname=" + local + \
        " -c supervisor.supervisors=" + cli_worker_list(worker_nodes)
    return config

def cli_worker_list(worker_nodes):
    w_list = "\"[\\\""
    
    for i in worker_nodes:
        w_list += i
        if i is not worker_nodes[-1]:
            w_list += "\",\""
    w_list += "\\\"]\""

    return w_list


def deploy_zk_nimbus(node, worker_nodes):
    # Start the zookeeper server
    zk_start_command = " 'zkServer.sh start'"
    os.system("ssh " + node + zk_start_command)
    
    print("Waiting for the zookeeper server to initialize...")
    time.sleep(5)

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
    for i in nodes:
        # Create local storage folder
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-local'")
        os.system("ssh " + i + " 'mkdir -p /local/ddps2016/storm-logs'")
        
        time.sleep(3)
        worker_start_command = \
            " 'screen -d -m storm supervisor" + \
            storm_cli_config(zk_nimbus_node, nodes, i) + "'"
 
        print("Deploying worker " + i)
        os.system("ssh " + i + worker_start_command)

def deploy_generator(node, gen_rate, reservation_id):
    # Start in screen to check output (only program that does not log to file)
    generator_start_command = \
	" 'screen -d -m python3 /home/ddps2016/DPS1/generator/benchmark_driver.py " + \
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

def submit_topology(nimbus_node, generator_node, mongo_node, num_workers, worker_nodes):
    print("Waiting for the storm cluster to initialize...")
    time.sleep(15)
    
    submit_command = \
        "cd /home/ddps2016/DPS1/storm_bench; make submit" + \
        " ZK_ADDRESS=" + nimbus_node + \
        " NIMBUS_ADDRESS=" + nimbus_node + \
        " INPUT_ADRESS=" + generator_node + \
        " INPUT_PORT=5555" + \
        " MONGO_ADRESS=" + mongo_node + \
        " NUM_WORKERS=" + str(num_workers) + \
        " WORKER_LIST=" + cli_worker_list(worker_nodes)

    print("Submitting topology to the cluster")
    os.system(submit_command)

def kill_cluster(zk_nimbus_node, mongo_node, worker_nodes):
    kill_command = \
    	"cd /home/ddps2016/DPS1/storm_bench; make kill" + \
    	" ZK_ADDRESS=" + zk_nimbus_node + \
    	" NIMBUS_ADDRESS=" + zk_nimbus_node
    print("Killing topology")
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
    if input("Clean logs?") == "y":
        os.system("ssh " + zk_nimbus_node + " 'rm -r /local/ddps2016/storm-logs/*'")
        for i in worker_nodes:
            os.system("ssh " + i + " 'rm -r /local/ddps2016/storm-logs/*'")

        os.system("rm /home/ddps2016/zookeeper/logs/*")
        os.system("rm /home/ddps2016/mongo/log/*")


def deploy_all(available_nodes, gen_rate, reservation_id):
    assert len(available_nodes) > 3
    # Assign nodes
    zk_nimbus_node = available_nodes[0]
    generator_node = available_nodes[1]
    mongo_node = available_nodes[2]
    worker_nodes = available_nodes[3:]
    num_workers = len(worker_nodes)

    # Deploy mongo server
    deploy_mongo(mongo_node)

    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node, worker_nodes)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Deploy data input generator
    deploy_generator(generator_node, gen_rate, reservation_id)

    # Submit topology to the cluster
    submit_topology(zk_nimbus_node, generator_node, mongo_node, num_workers, worker_nodes)

    while True:
        if input("Type \"k\" to kill the cluster\n") == "k":
            kill_cluster(zk_nimbus_node, mongo_node, worker_nodes)
            break
		
