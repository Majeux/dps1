import os

def deploy_zookeeper(node):
    os.system("ssh " + node + "'zookeeper/bin/zkServer.sh start'")

def deploy_nimbus(node):
    os.system("ssh " + node + "'storm/bin/storm nimbus'")

def deploy_workers(nodes):
    for i in nodes:
    os.system("ssh " + node + "'storm/bin/storm supervisor'")


def deploy_all(available_nodes):
    deploy_zookeeper(available_nodes[0])
    deploy_nimbus(available_nodes[1])
    deploy_workers(available_nodes[2:])
