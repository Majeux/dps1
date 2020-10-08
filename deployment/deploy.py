import os
import time

BUDGET = 50000
NUM_GENERATORS = 16

def deploy_zk_nimbus(node):
    # Start the zookeeper server
    zk_start_command = "'zkServer.sh start'"
    os.system("ssh " + node + zk_start_command)
    # Wait for the zk server to start
    time.sleep(5)

    #Start the nimbus
    nimbus_start_command = (
        " 'storm nimbus "
        "-c storm.zookeeper.servers=\"[\\\"" + "localhost" + "\\\"]\" "
        "-c nimbus.seeds=\"[\\\"" + "localhost" + "\\\"]\"'"
    )

    print("Deploying nimbus on " + node)
    os.system("ssh " + node + nimbus_start_command)

def deploy_workers(nodes, zk_node, nimbus_node):
    worker_start_command = (
        " 'storm supervisor "
        "-c storm.zookeeper.servers=\"[\\\"" + zk_node + "\\\"]\" "
        "-c nimbus.seeds=\"[\\\"" + nimbus_node + "\\\"]\"'"
    )

    for i in nodes:
        print("Deploying worker " + i)
        os.system("ssh " + node + worker_start_command)

def deploy_generator(node, gen_rate, reservation_id):
    # Wait for the storm cluster to initialise
    sleep(10)

    # Start in screen to check output (only program that does not log to file)
    generator_start_command = (
        " 'screen -d -m python3 /home/ddps2016/dps1/generator/benchmark_driver.py "
        str(BUDGET) + " " + str(gen_rate) + " " + str(NUM_GENERATORS) + " clock.liacs.nl'"
    )

    print("Deploying generator on " + node)
    os.system("ssh " + node + generator_start_command)

def deploy_mongo(node):
    mongo_start_command = " 'numactl --interleave=all mongod --config ~/mongo/mongodb.conf'"

    print("Deploying mongo server on " + node)
    os.system("ssh " + node + mongo_start_command);

def submit_topology(nimbus_node, generator_node, mongo_node, num_workers):
    submit_command = (
        "cd /home/ddps2016/DPS1/storm_bench; make submit",
        " ZK_ADDRESS=" + nimbus_node,
        " NIMBUS_ADDRESS=" + nimbus_node,
        " INPUT_ADRESS=" + generator_node,
        " INPUT_PORT=5555",
        " MONGO_ADRESS=" + mongo_node,
        " NUM_WORKERS=" + str(num_workers)
    )

    print("Submitting topology to the cluster")
    System.os(submit_command)

def deploy_all(available_nodes, gen_rate, reservation_id):
    # Assign nodes
    zk_nimbus_node = available_nodes[0]
    generator_node = available_nodes[1]
    mongo_ntp_node = available_nodes[3]
    worker_nodes = available_nodes[4:]
    num_workers = len(worker_nodes)

    # Deploy mongo server
    deploy_mongo(mongo_ntp_node)

    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Deploy data input generator
    deploy_generator(generator_node)

    # Submit topology to the cluster
    submit_topology(zk_nimbus_node, generator_node, mongo_node, num_workers)

