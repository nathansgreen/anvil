# Not many C source files left now...
CSOURCES=blowfish.c md5.c openat.c
CPPSOURCES=string_counter.cpp istr.cpp new.cpp rofile.cpp rwfile.cpp stringset.cpp stringtbl.cpp
CPPSOURCES+=blob.cpp blob_buffer.cpp params.cpp sub_blob.cpp journal.cpp sys_journal.cpp transaction.cpp
CPPSOURCES+=simple_dtable.cpp simple_ctable.cpp simple_stable.cpp simple_ext_index.cpp memory_dtable.cpp
CPPSOURCES+=cache_dtable.cpp journal_dtable.cpp overlay_dtable.cpp ustr_dtable.cpp managed_dtable.cpp
CPPSOURCES+=dtable_factory.cpp ctable_factory.cpp index_factory.cpp token_stream.cpp toilet++.cpp
SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

.PHONY: all clean clean-all count count-all php

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

CFLAGS:=-Wall -Ifstitch/include $(CFLAGS)
LDFLAGS:=-Lfstitch/obj/kernel/lib -lpatchgroup -Wl,-R,$(PWD)/fstitch/obj/kernel/lib $(LDFLAGS)

all: tags main io_count.so

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

%.o: %.cpp
	g++ -c $< -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

libtoilet.so: libtoilet.o fstitch/obj/kernel/lib/libpatchgroup.so
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

clean:
	rm -f main libtoilet.so libtoilet.a io_count.so *.o .depend tags

clean-all: clean
	php/clean

count:
	wc -l *.[ch] *.cpp | sort -n

count-all:
	wc -l *.[ch] *.cpp php/*.[ch] invite/*.php | sort -n

php:
	if [ -f php/Makefile ]; then make -C php; else php/compile; fi

.depend: $(SOURCES) $(HEADERS) main.c
	g++ -MM $(CFLAGS) $(CPPFLAGS) *.c *.cpp > .depend

tags: $(SOURCES) main.c $(HEADERS)
	ctags -R

-include .depend
