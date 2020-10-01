STORM_DIR	= /home/rijeun/Documents/DPS/storm

LIBS		=$(STORM_DIR)/lib/storm-client-2.2.0.jar:$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar:$(STORM_DIR)/lib/storm-starter-2.2.0.jar:$(STORM_DIR)/lib/storm-shaded-deps-2.2.0.jar

RUN_LIBS	=$(STORM_DIR)/lib/storm-starter-2.2.0.jar,$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar 
BUILDDIR	= ./build
SRC			= ./AggregationTest/AggregateSum.java
JARNAME		= ./AggregateSum.jar

all: test

test:
	javac -cp $(LIBS) $(SRC)
	jar cvf $(JARNAME) AggregationTest

run:
	$(STORM_DIR)/bin/storm local --jars $(RUN_LIBS) ./AggregateSum.jar AggregationTest.AggregateSum
