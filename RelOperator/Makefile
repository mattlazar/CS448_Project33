JDKPATH = /usr
LIBPATH = lib/bufmgr.jar:lib/diskmgr.jar:lib/heap.jar:lib/index.jar

CLASSPATH = .:..:src/:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = compile #xx

all: $(PROGS)

compile:src/*/*.java #src/*/*.java
	$(JAVAC) -cp $(CLASSPATH) -d bin src/relop/FileScan.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.ROTest
	$(JAVA) -cp $(CLASSPATH):bin tests.QEPTest src/tests/SampleData

