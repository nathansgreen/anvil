CSOURCES=$(wildcard *.c)
CPPSOURCES=$(wildcard *.cpp)
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean

all: tags toilet

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -O2 $(CFLAGS) $(CPPFLAGS)

toilet: $(OBJECTS)
ifeq ($(CPPOBJECTS),)
	gcc -o toilet $(OBJECTS)
else
	g++ -o toilet $(OBJECTS)
endif

clean:
	rm -f toilet *.o .depend tags

.depend: $(SOURCES)
ifeq ($(CPPSOURCES),)
	gcc -MM *.c > .depend
else
	g++ -MM *.c *.cpp > .depend
endif

tags: $(SOURCES) $(HEADERS)
	ctags -R

-include .depend
