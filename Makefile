CSOURCES=blowfish.c journal.c md5.c openat.c str_tbl.c
CPPSOURCES=counted_stringset.cpp istr.cpp stringset.cpp transaction.cpp
CPPSOURCES+=blob.cpp blob_buffer.cpp params.cpp sub_blob.cpp sys_journal.cpp
CPPSOURCES+=simple_dtable.cpp simple_ctable.cpp simple_stable.cpp simple_ext_index.cpp
CPPSOURCES+=journal_dtable.cpp overlay_dtable.cpp ustr_dtable.cpp managed_dtable.cpp
CPPSOURCES+=dtable_factory.cpp ctable_factory.cpp index_factory.cpp toilet++.cpp
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean clean-all count count-all php

CFLAGS:=-Wall -Ifstitch/include $(CFLAGS)
LDFLAGS:=-Lfstitch/obj/kernel/lib -lpatchgroup -Wl,-R,$(PWD)/fstitch/obj/kernel/lib $(LDFLAGS)

all: tags main

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

libtoilet.so: $(OBJECTS) fstitch/obj/kernel/lib/libpatchgroup.so
	g++ -shared -o libtoilet.so $(OBJECTS) -ldl $(LDFLAGS)

main: libtoilet.so main.o main++.o
	g++ -o main main.o main++.o -Wl,-R,$(PWD) -L. -ltoilet -lreadline -ltermcap $(LDFLAGS)

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

.depend: $(SOURCES) $(HEADERS) main.c
	g++ -MM $(CFLAGS) *.c *.cpp > .depend

tags: $(SOURCES) main.c $(HEADERS)
	ctags -R

-include .depend
