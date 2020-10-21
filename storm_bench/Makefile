# Java stuff
COMPILE_LIBS	= $(shell find compile_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/:/g')
RUN_LIBS		= $(shell find run_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/,/g')
SRC_DIR			= ./aggregation
SRC_FILES		= $(shell find $(SRC_DIR) -name "*.java")
JAR_NAME		= ./aggregate.jar

# Command line arguments
INPUT_ADRESS	= localhost
INPUT_PORT		= 5555
MONGO_ADRESS	= localhost
NUM_WORKERS		= 1
NIMBUS_ADDRESS	= localhost
ZK_ADDRESS		= localhost
WORKER_LIST     = []

all: build

build: $(SRC_FILES)
	make clean
	javac -cp $(COMPILE_LIBS) $(SRC_FILES)
	jar cvf $(JAR_NAME) $(SRC_DIR)

submit: build
	storm jar \
	-c storm.zookeeper.servers="[\"$(ZK_ADDRESS)\"]" \
	-c nimbus.seeds="[\"$(NIMBUS_ADDRESS)\"]" \
        -c supervisor.supervisors=$(WORKER_LIST) \
        --jars $(RUN_LIBS) $(JAR_NAME) aggregation.AggregateSum \
	$(INPUT_ADRESS) $(INPUT_PORT) $(MONGO_ADRESS) $(NUM_WORKERS) $(NTP_ADDRESS) 

kill:
	storm kill \
	-c storm.zookeeper.servers="[\"$(ZK_ADDRESS)\"]" \
        -c nimbus.seeds="[\"$(NIMBUS_ADDRESS)\"]" \
	"agsum"

clean:
	rm -f $(JARNAME)
	rm -f $(SRC_DIR)/*.class
