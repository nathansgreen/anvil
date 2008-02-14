CSOURCES=blowfish.c hash_map.c hash_set.c openat.c toilet.c vector.c
CPPSOURCES=diskhash.cpp disktree.cpp index.cpp memcache.cpp multimap.cpp
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

libtoilet.so: $(OBJECTS)
	g++ -shared -o libtoilet.so $(OBJECTS) -ldl $(LDFLAGS)

toilet: libtoilet.so main.c
	gcc -o toilet main.c $(CFLAGS) -Wl,-R,. -L. -ltoilet -lreadline -ltermcap $(LDFLAGS)

clean:
	rm -f toilet libtoilet.so *.o .depend tags

count:
	wc -l *.c *.cpp *.h | sort -n

.depend: $(SOURCES) main.c
	g++ -MM *.c *.cpp > .depend

tags: $(SOURCES) main.c $(HEADERS)
	ctags -R

-include .depend
