STORM_DIR	= /home/rijeun/Documents/DPS/storm

COMPILE_LIBS	= $(shell find compile_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/:/g')
RUN_LIBS		= $(shell find run_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/,/g')

BUILDDIR	= ./build
SRC			= ./aggregation/AggregateSum.java
JARNAME		= ./aggregate.jar
MONGO_STR	= mongodb://storm:test@127.0.0.1:27017/&authSource=results

all: test

test:
	make clean
	javac -cp $(COMPILE_LIBS) $(SRC)
	jar cvf $(JARNAME) aggregation

submit: test
	$(STORM_DIR)/bin/storm jar --jars $(RUN_LIBS) ./aggregate.jar aggregation.AggregateSum localhost 5555 localhost 1 

kill:
	$(STORM_DIR)/bin/storm kill "agsum"

clean:
	rm -f $(JARNAME)
