LIBS		=../../storm/lib/storm-client-2.2.0.jar:../../storm/lib-tools/sql/runtime/storm-sql-runtime-2.2.0.jar:../../storm/lib/storm-starter-2.2.0.jar:../../storm/lib/storm-shaded-deps-2.2.0.jar

BUILDDIR	= ./build
SRC			= ./AggregateSum.java
JARNAME		= ./AggregateSum.jar

all: test

test:
	javac -cp $(LIBS) -d $(BUILDDIR) $(SRC)
	jar cvf $(JARNAME) $(BUILDDIR)/*
