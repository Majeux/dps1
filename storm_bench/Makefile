STORM_DIR	= /home/rijeun/Documents/DPS/storm

LIBS		=$(STORM_DIR)/lib/storm-client-2.2.0.jar:$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar:$(STORM_DIR)/lib/storm-starter-2.2.0.jar:$(STORM_DIR)/lib/storm-shaded-deps-2.2.0.jar

RUN_LIBS	=$(STORM_DIR)/lib/storm-starter-2.2.0.jar,$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar 
BUILDDIR	= ./build
SRC			= ./aggregation/AggregateSum.java
JARNAME		= ./aggregate.jar

all: test

test:
	javac -cp $(LIBS) $(SRC)
	jar cvf $(JARNAME) aggregation

submit:
	$(STORM_DIR)/bin/storm jar --jars $(RUN_LIBS) ./aggregate.jar aggregation.AggregateSum localhost 5555 1 
