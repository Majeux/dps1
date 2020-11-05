# DPS assignment 1

This is a reproduction of some experiments in <https://arxiv.org/abs/1802.08496> on the das5 cluster.
The project is subdivided into 5 components:

- **generator:** *A custom data generator which connects to the storm cluster with a TCP socket.* 
- **storm\_bench:** *A custom topology that aggregates some field by key, similar to the workload from the paper.*
- **deployment:** *Scripts to deploy the above tools onto the das5 cluster.*

## Benchmark driver
Python program that generates tuples in JSON format. The data generated are simplified purchases.
Tuples have the fields "GemID", "price" and "event_time", representing a Gem pack, 
the price of that gem pack, and the time of purchase respectively. The JSONs are serialized,
and sent over a TCP socket, to be recieved by a SocketSpout within a storm topology. The data 
generation is done on a configurable number of child processes.

## Storm topology
A storm topology, built using the stream API. Receives the aforementioned JSON tuples, and 
aggregates the prices, grouped by GemID. Since the experiments we are reproducing measure the
event time of such aggregation operations, some extra logic is performed - mid aggragation -
to keep track of event times. The results of these aggregations (aggregate price, latency) is
inserted into a mongodb database for later reference.

## Das5 deployment
Scripts that aid in automated deployment of the above onto the das5 cluster. The scripts 
perform the following tasks:
- Nodes are reserved on the das5.
- A storm cluster is set up.
- A mongoDB server is initialized.
- The data generator is initialized.
- The aggregation topology is submitted to the storm cluster.

## Configs
Configuration files for the various software packages that were used in the cluster

## Results
The raw data, plots, etc. of the various benchmarks we performed. See the report for more info.
