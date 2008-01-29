CSOURCES=$(wildcard *.c)
CPPSOURCES=$(wildcard *.cpp)
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean count

all: tags toilet

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

toilet: $(OBJECTS)
ifeq ($(CPPOBJECTS),)
	gcc -o toilet $(OBJECTS) -ldl $(LDFLAGS)
else
	g++ -o toilet $(OBJECTS) -ldl $(LDFLAGS)
endif

clean:
	rm -f toilet *.o .depend tags

count:
	wc -l *.c *.cpp *.h | sort -n

.depend: $(SOURCES)
ifeq ($(CPPSOURCES),)
	gcc -MM *.c > .depend
else
	g++ -MM *.c *.cpp > .depend
endif

tags: $(SOURCES) $(HEADERS)
	ctags -R

-include .depend
