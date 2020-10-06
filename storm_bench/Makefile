COMPILE_LIBS	= $(shell find compile_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/:/g')
RUN_LIBS		= $(shell find run_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/,/g')

SRC_DIR			= ./aggregation
JAR_MAIN		= AggregateSum.java
JAR_NAME		= ./aggregate.jar
MONGO_STR		= mongodb://storm:test@127.0.0.1:27017/&authSource=results

all: build

build:
	make clean
	javac -cp $(COMPILE_LIBS) $(SRC_DIR)/$(JAR_MAIN)
	jar cvf $(JAR_NAME) $(SRC_DIR)

# ARGS:
# class, input_address, input_port, mongo_address, NTP_address, num_workers
submit: build
	storm jar --jars $(RUN_LIBS) $(JAR_NAME) aggregation.AggregateSum localhost 5555 localhost localhost 1 

kill:
	storm kill "agsum"

clean:
	rm -f $(JARNAME)
