import os
import time

BUDGET = 50000
NUM_GENERATORS = 16

def deploy_zk_nimbus(node):
    # Start the zookeeper server
    zk_start_command = " 'zkServer.sh start'"
    os.system("ssh " + node + zk_start_command)
    
    print("Waiting for the zookeeper server to initialize...")
    time.sleep(5)

    #Start the nimbus
    nimbus_start_command = \
        " 'screen -d -m storm nimbus" + \
        " -c storm.zookeeper.servers=\"[\\\"" + node + "\\\"]\"" + \
        " -c nimbus.seeds=\"[\\\"" + node + "\\\"]\"" + \
	" -c storm.local.hostname=" + node + " &'"

    print("Deploying nimbus on " + node)
    os.system("ssh " + node + nimbus_start_command)

def deploy_workers(nodes, zk_nimbus_node):
    for i in nodes:
        worker_start_command = \
            " 'screen -d -m storm supervisor" + \
            " -c storm.zookeeper.servers=\"[\\\"" + zk_nimbus_node + "\\\"]\"" + \
            " -c nimbus.seeds=\"[\\\"" + zk_nimbus_node + "\\\"]\"" + \
            " -c storm.local.hostname=" + i + " &'"
        
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
    os.system("ssh " + node + " 'rsync -r --delete /var/scratch/ddps2016/mongo_data/ /local/ddps2016/mongo_data/'")

    mongo_start_command = \
	" 'screen -d -m numactl --interleave=all" + \
	" mongod --config /home/ddps2016/mongo/mongodb.conf &'"

    print("Deploying mongo server on " + node)
    os.system("ssh " + node + mongo_start_command);

def submit_topology(nimbus_node, generator_node, mongo_node, num_workers):
    print("Waiting for the storm cluster to initialize...")
    time.sleep(15)
    
    submit_command = \
        "cd /home/ddps2016/DPS1/storm_bench; make submit" + \
        " ZK_ADDRESS=" + nimbus_node + \
        " NIMBUS_ADDRESS=" + nimbus_node + \
        " INPUT_ADRESS=" + generator_node + \
        " INPUT_PORT=5555" + \
        " MONGO_ADRESS=" + mongo_node + \
        " NUM_WORKERS=" + str(num_workers)

    print("Submitting topology to the cluster")
    os.system(submit_command)

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
    deploy_zk_nimbus(zk_nimbus_node)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Deploy data input generator
    deploy_generator(generator_node, gen_rate, reservation_id)

    # Submit topology to the cluster
    submit_topology(zk_nimbus_node, generator_node, mongo_node, num_workers)

    while True:
        if input("Type \"k\" to kill the cluster\n") == "k":
            kill_command = \
	    	"cd /home/ddps2016/DPS1/storm_bench; make kill" + \
            	" ZK_ADDRESS=" + zk_nimbus_node + \
            	" NIMBUS_ADDRESS=" + zk_nimbus_node
            print("Killing topology")
            os.system(kill_command)
            print("Spouts disabled. Waiting 30 seconds to process leftover tuples")
            time.sleep(35)
            os.system("preserve -c $(preserve -llist | grep ddps2016 | cut -f 1)")
            
            if input("Clean logs?") == "y":
                os.system("rm -r /var/scratch/ddps2016/stormlogs/*")
                os.system("rm /home/ddps2016/zookeeper/logs/*")
            break
		
