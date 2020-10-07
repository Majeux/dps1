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
    os.system("ssh " + node + nimbus_start_command)

def deploy_workers(nodes, zk_node, nimbus_node):
    worker_start_command = (
        " 'storm supervisor "
        "-c storm.zookeeper.servers=\"[\\\"" + zk_node + "\\\"]\" "
        "-c nimbus.seeds=\"[\\\"" + nimbus_node + "\\\"]\"'"
    )

    for i in nodes:
        os.system("ssh " + node + worker_start_command)

def deploy_generator(node, gen_rate, reservation_id):
    # Wait for the storm cluster to initialise
    sleep(10)
    
    generator_start_command = (
        " 'screen -d -m python3 /home/ddps2016/dps1/generator/benchmark_driver.py "
        str(BUDGET) + " " + str(gen_rate) + " " + str(NUM_GENERATORS) + " clock.liacs.nl'"
    )
    os.system("ssh " + node + generator_start_command)

def deploy_mongo(node)

def deploy_all(available_nodes, gen_rate, reservation_id):
    # Assign nodes
    zk_nimbus_node = available_nodes[0]
    generator_node = available_nodes[1]
    mongo_ntp_node = available_nodes[3]
    worker_nodes = available_nodes[4:]

    # Deploy mongo server
    deploy_mongo_ntp(mongo_ntp_node)

    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node)
    deploy_workers(worker_nodes, zk_nimbus_node)


    deploy_generator()
    return True
