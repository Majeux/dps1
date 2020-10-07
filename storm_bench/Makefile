# Java stuff
COMPILE_LIBS	= $(shell find compile_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/:/g')
RUN_LIBS		= $(shell find run_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/,/g')
SRC_DIR			= ./aggregation
JAR_MAIN		= AggregateSum.java
JAR_NAME		= ./aggregate.jar

# Command line arguments
INPUT_ADRESS	= localhost
INPUT_PORT		= 5555
MONGO_ADRESS	= localhost
NTP_ADDRESS		= localhost
NUM_WORKERS		= 1
NIMBUS_ADDRESS	= localhost
ZK_ADDRESS		= localhost

all: build

build: $(SRC_DIR)/$(JAR_MAIN)
	make clean
	javac -cp $(COMPILE_LIBS) $(SRC_DIR)/$(JAR_MAIN)
	jar cvf $(JAR_NAME) $(SRC_DIR)

# class, input_address, input_port, mongo_address, NTP_address, num_workers
submit: build
	storm jar \
	-c storm.zookeeper.servers="[\"$(ZK_ADDRESS)\"]" \
	-c nimbus.seeds="[\"$(NIMBUS_ADDRESS)\"]" \
	--jars $(RUN_LIBS) $(JAR_NAME) aggregation.AggregateSum \
	$(INPUT_ADRESS) $(INPUT_PORT) $(MONGO_ADRESS) $(NTP_ADDRESS) $(NUM_WORKERS) 

kill:
	storm kill "agsum"

clean:
	rm -f $(JARNAME)
