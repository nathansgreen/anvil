# Not many C source files left now...
CSOURCES=blowfish.c md5.c openat.c
# NOTE: list all factory-constructible classes (dtables, etc.) before the
# corresponding factory registries. See factory_impl.h for the reason.
# For the same reason, transaction.cpp must come after sys_journal.cpp here.
CPPSOURCES=string_counter.cpp istr.cpp new.cpp rofile.cpp rwfile.cpp stringset.cpp stringtbl.cpp util.cpp
CPPSOURCES+=blob.cpp blob_buffer.cpp params.cpp sub_blob.cpp index_blob.cpp journal.cpp sys_journal.cpp transaction.cpp
CPPSOURCES+=simple_dtable.cpp simple_ctable.cpp simple_stable.cpp simple_ext_index.cpp memory_dtable.cpp
CPPSOURCES+=btree_dtable.cpp cache_dtable.cpp journal_dtable.cpp overlay_dtable.cpp ustr_dtable.cpp
CPPSOURCES+=managed_dtable.cpp array_dtable.cpp exception_dtable.cpp usstate_dtable.cpp column_ctable.cpp
CPPSOURCES+=token_stream.cpp dtable_factory.cpp ctable_factory.cpp index_factory.cpp toilet.cpp toilet++.cpp
CPPSOURCES+=stlavlmap/tree.cpp
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean clean-all count count-all php

-include config.mak

CFLAGS:=-Wall $(FSTITCH_CFLAGS) $(CONFIG_CFLAGS) $(CFLAGS)
LDFLAGS:=$(FSTITCH_LDFLAGS) $(CONFIG_LDFLAGS) $(LDFLAGS)

ifeq ($(findstring -pg,$(CFLAGS)),-pg)
ifeq ($(findstring -pg,$(LDFLAGS)),)
# Add -pg to LDFLAGS if it's in CFLAGS and not LDFLAGS
LDFLAGS:=-pg $(LDFLAGS)
endif
# We can't actually do this without a small change to openat.c;
# dlsym(RTLD_NEXT, ...) crashes when libc is linked statically
#ifeq ($(findstring -lc_p,$(LDFLAGS)),)
#LDFLAGS:=-lc_p $(LDFLAGS)
#endif
endif

# On 64-bit architectures, -fpic is required
ifeq ($(findstring 64,$(shell uname -m)),64)
CFLAGS:=-fpic $(CFLAGS)
endif

all: config.mak tags main io_count.so

%.o: %.c
	gcc -c $< -o $@ -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -o $@ -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

libtoilet.so: libtoilet.o $(FSTITCH_LIB)
	g++ -shared -o $@ $< -ldl $(LDFLAGS)

libtoilet.o: $(OBJECTS)
	ld -r -o $@ $^

# Make libtoilet.a from libtoilet.o instead of $(OBJECTS) directly so that
# classes not directly referenced still get included and register themselves
# to be looked up via factory registries, which is how most *tables work.
libtoilet.a: libtoilet.o
	ar csr $@ $<

ifeq ($(findstring -pg,$(CFLAGS)),-pg)
# Link statically if we are profiling; gprof won't profile shared library code
main: libtoilet.a main.o main++.o
	g++ -o $@ main.o main++.o libtoilet.a -lreadline -ltermcap $(LDFLAGS)
else
main: libtoilet.so main.o main++.o
	g++ -o $@ main.o main++.o -Wl,-R,$(PWD) -L. -ltoilet -lreadline -ltermcap $(LDFLAGS)
endif

io_count.so: io_count.o
	gcc -shared -o $@ $< -ldl $(LDFLAGS)

medic: medic.o md5.o
	gcc -o $@ $^

clean:
	rm -f config.h config.mak main libtoilet.so libtoilet.a io_count.so medic *.o stlavlmap/*.o .depend tags

clean-all: clean
	php/clean

count:
	wc -l *.[ch] *.cpp | sort -n

count-all:
	wc -l *.[ch] *.cpp php/*.[ch] invite/*.php | sort -n

php:
	if [ -f php/Makefile ]; then make -C php; else php/compile; fi

config.h: config.mak

config.mak: configure
	./configure --reconfigure

.depend: $(SOURCES) $(HEADERS) config.h main.c main++.cpp
	g++ -MM $(CFLAGS) $(CPPFLAGS) *.c *.cpp > .depend

tags: $(SOURCES) main.c $(HEADERS)
	ctags -R

-include .depend
