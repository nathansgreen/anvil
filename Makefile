CSOURCES=blowfish.c hash_map.c hash_set.c journal.c md5.c openat.c toilet.c vector.c
CPPSOURCES=diskhash.cpp disktree.cpp index.cpp memcache.cpp multimap.cpp
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean clean-all count count-all php

CFLAGS:=-Ifstitch/include $(CFLAGS)
LDFLAGS:=-Lfstitch/obj/kernel/lib -lpatchgroup -Wl,-R,$(PWD)/fstitch/obj/kernel/lib $(LDFLAGS)

all: tags main

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

libtoilet.so: $(OBJECTS) fstitch/obj/kernel/lib/libpatchgroup.so
	g++ -shared -o libtoilet.so $(OBJECTS) -ldl $(LDFLAGS)

main: libtoilet.so main.c
	gcc -o main main.c $(CFLAGS) -Wl,-R,$(PWD) -L. -ltoilet -lreadline -ltermcap $(LDFLAGS)

clean:
	rm -f main libtoilet.so *.o .depend tags

clean-all: clean
	php/clean

count:
	wc -l *.[ch] *.cpp | sort -n

count-all:
	wc -l *.[ch] *.cpp php/*.[ch] invite/*.php | sort -n

php:
	if [ -f php/Makefile ]; then make -C php; else php/compile; fi

.depend: $(SOURCES) main.c
	g++ -MM *.c *.cpp > .depend

tags: $(SOURCES) main.c $(HEADERS)
	ctags -R

-include .depend
