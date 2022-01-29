
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall # -g

all: correctness persistence

correctness: kvstore.o correctness.o MemTable.o fuctions_in_kvstore.o file_operation.o

persistence: kvstore.o persistence.o MemTable.o fuctions_in_kvstore.o file_operation.o

clean:
	-rm -f correctness persistence *.o
