# Makefile.

SHELL = '/bin/sh'

CXX = g++

CXXFLAGS = -g -O2 -I/home/eecs/benh/local/include -I/home/eecs/benh/local/include

LIBEV = /scratch/benh/libprocess/examples/../third_party/libev-3.8

BOOST = /scratch/benh/libprocess/examples/../third_party/boost-1.37.0

HTTP_PARSER = http-parser

# Add .. to CXXFLAGS.
CXXFLAGS += -I/scratch/benh/libprocess/examples/..

# Add libev to CXXFLAGS.
CXXFLAGS += -I$(LIBEV)

# Add Boost to CXXFLAGS.
CXXFLAGS += -I$(BOOST)

# Add dependency tracking to CXXFLAGS.
CXXFLAGS += -MMD -MP

# Add directory for libprocess.
LDFLAGS += -L..  -L/home/eecs/benh/local/lib -L/home/eecs/benh/local/lib

# Add libev (as static) and libprocess to LIBS.
LIBS += -lprocess /scratch/benh/libprocess/examples/../third_party/libev-3.8/.libs/libev.a -llithe -lht -lpthread 

HTTPD_OBJ = httpd.o

HTTPD = httpd

EXAMPLES_OBJ = $(HTTPD_OBJ)

EXAMPLES = $(HTTPD)

default: all

-include $(patsubst %.o, %.d, $(EXAMPLES_OBJ))

$(EXAMPLES_OBJ): %.o: %.cpp 
	$(CXX) -c $(CXXFLAGS) -o $@ $<

/scratch/benh/libprocess/examples/http-parser/http_parser.o:
	$(MAKE) -C /scratch/benh/libprocess/examples/http-parser

$(HTTPD): /scratch/benh/libprocess/examples/http-parser/http_parser.o

$(HTTPD): $(HTTPD_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

all: $(EXAMPLES)

clean:
	$(MAKE) -C /scratch/benh/libprocess/examples/http-parser clean
	rm -rf $(patsubst %.o, %.d, $(EXAMPLES_OBJ))
	rm -rf $(EXAMPLES_OBJ)
	rm -rf $(EXAMPLES)

.PHONY: default all clean