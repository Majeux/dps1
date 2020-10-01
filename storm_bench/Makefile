STORM_DIR	= /home/rijeun/Documents/DPS/storm

LIBS		=$(STORM_DIR)/lib/storm-client-2.2.0.jar:$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar:$(STORM_DIR)/lib/storm-starter-2.2.0.jar:$(STORM_DIR)/lib/storm-shaded-deps-2.2.0.jar

BUILDDIR	= ./build
SRC			= ./AggregateSum.java
JARNAME		= ./AggregateSum.jar

all: test

test:
	javac -cp $(LIBS) -d $(BUILDDIR) $(SRC)
	jar cvf $(JARNAME) $(BUILDDIR)/*

run:
	$(STORM_DIR)/bin/storm jar --jars $(STORM_DIR)/lib/storm-starter-2.2.0.jar:$(STORM_DIR)/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar ./* build.AggregateSum
