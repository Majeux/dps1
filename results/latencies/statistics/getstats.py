from numpy import quantile
from numpy import mean
from os import listdir
from os.path import isfile, join

QUANTILES = [0.9, 0.95, 0.99]
FIELDS = ["throughput", "avg_latency", "min_latency", "max_latency", "quantiles(90,95,99)"]

files = dict()

files[2] = [f for f in listdir(".") if ".res" in f and "2node" in f]
files[4] = [f for f in listdir(".") if ".res" in f and "4node" in f]
files[8] = [f for f in listdir(".") if ".res" in f and "8node" in f]

for num_nodes, results in files.items():
    print(num_nodes, " nodes:")
    print(", ".join(FIELDS))
    
    for i in sorted(results, reverse=True):
        with open(i) as f:
            latencies = [float(line.rstrip()) for line in f]
        
        percentage = i[4:7]
        avg_lat = mean(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)
        quantiles = quantile(latencies, QUANTILES)
        
        print(percentage, avg_lat, min_lat, max_lat, quantiles)
    print()
