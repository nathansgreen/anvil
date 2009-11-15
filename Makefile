# Not many C source files left now...
CSOURCES=blowfish.c md5.c openat.c

# library stuff
LIBRARIES=anvil.cpp bg_token.cpp blob_buffer.cpp blob.cpp dtable.cpp index_blob.cpp istr.cpp
LIBRARIES+=journal.cpp new.cpp params.cpp rofile.cpp rwfile.cpp string_counter.cpp stringtbl.cpp
LIBRARIES+=sys_journal.cpp toilet.cpp token_stream.cpp stlavlmap/tree.cpp util.cpp

# dtables
DTABLES=array_dtable.cpp btree_dtable.cpp bloom_dtable.cpp cache_dtable.cpp deltaint_dtable.cpp
DTABLES+=exception_dtable.cpp fixed_dtable.cpp journal_dtable.cpp keydiv_dtable.cpp linear_dtable.cpp
DTABLES+=managed_dtable.cpp memory_dtable.cpp overlay_dtable.cpp simple_dtable.cpp smallint_dtable.cpp
DTABLES+=usstate_dtable.cpp ustr_dtable.cpp

# ctables, stables, and external indices
MISC_STUFF=column_ctable.cpp simple_ctable.cpp simple_stable.cpp simple_ext_index.cpp

# factory registries and transactions (see note below)
FACTORIES=dtable_factory.cpp ctable_factory.cpp index_factory.cpp transaction.cpp

UNAME_S:=$(shell uname -s)
UNAME_M:=$(shell uname -m)

# NOTE: all factory-constructible classes (dtables, etc.) must come before
# the corresponding factory registries. See factory_impl.h for the reason.
# For the same reason, transaction.cpp must come after sys_journal.cpp here.
# Any static listening_dtable_warehouses (e.g. journal_dtable_warehouse)
# must also be listed after sys_journal.cpp, if the global sys_journal is
# not explicitly deinitialized by client code.

ifeq ($(UNAME_S),Darwin)
# Well, except that on OS X they must be listed in the other order
CPPSOURCES=$(FACTORIES) $(MISC_STUFF) $(DTABLES) $(LIBRARIES)
else
CPPSOURCES=$(LIBRARIES) $(DTABLES) $(MISC_STUFF) $(FACTORIES)
endif

SOURCES=$(CSOURCES) $(CPPSOURCES)

HEADERS=$(wildcard *.h)

COBJECTS=$(CSOURCES:.c=.o)
CPPOBJECTS=$(CPPSOURCES:.cpp=.o)
OBJECTS=$(COBJECTS) $(CPPOBJECTS)

MAIN_SRC=main.c main_util.cpp main_perf.cpp main_test.cpp tpch.cpp
MAIN_OBJ=main.o main_util.o main_perf.o main_test.o tpch.o

.PHONY: all clean clean-all count count-all php

-include config.mak

PCFLAGS=-Wall $(FSTITCH_CFLAGS) $(CONFIG_CFLAGS)

CFLAGS:=$(PCFLAGS) $(CFLAGS)
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

# Mac OS X is special
ifeq ($(UNAME_S),Darwin)
SO=dylib
SHARED=-dynamiclib
RTP=
PIC=-fPIC
else
SO=so
SHARED=-shared
RTP=-Wl,-R,$(PWD)
PIC=-fpic
endif

# On 64-bit architectures, -fpic is required
ifeq ($(findstring 64,$(UNAME_M)),64)
CFLAGS:=$(PIC) $(CFLAGS)
endif

all: config.mak tags main io_count.$(SO)

%.o: %.c
	$(CC) -c $< -o $@ -O2 $(CFLAGS)

%.o: %.cpp
	$(CXX) -c $< -o $@ -O2 $(CFLAGS) -fno-exceptions -fno-rtti $(CPPFLAGS)

libanvil.$(SO): libanvil.o $(FSTITCH_LIB)
	$(CXX) $(SHARED) -o $@ $< -ldl -lpthread $(LDFLAGS)

libanvil.o: $(OBJECTS)
	$(LD) -r -o $@ $^

# Make libanvil.a from libanvil.o instead of $(OBJECTS) directly so that
# classes not directly referenced still get included and register themselves
# to be looked up via factory registries, which is how most *tables work.
libanvil.a: libanvil.o
	ar csr $@ $<

ifeq ($(findstring -pg,$(CFLAGS)),-pg)
# Link statically if we are profiling; gprof won't profile shared library code
main: libanvil.a $(MAIN_OBJ)
	$(CXX) -o $@ $(MAIN_OBJ) libanvil.a -lreadline -ltermcap $(LDFLAGS)
else
main: libanvil.$(SO) $(MAIN_OBJ)
	$(CXX) -o $@ $(MAIN_OBJ) $(RTP) -L. -lanvil -lreadline -ltermcap $(LDFLAGS)
endif

io_count.$(SO): io_count.o
	$(CC) $(SHARED) -o $@ $< -ldl $(LDFLAGS)

medic: medic.o md5.o
	$(CC) -o $@ $^

clean:
	rm -f config.h config.mak main libanvil.$(SO) libanvil.a io_count.$(SO) medic *.o stlavlmap/*.o .depend tags

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

.depend: $(SOURCES) $(MAIN_SRC) $(HEADERS) config.h
	$(CXX) -MM $(PCFLAGS) $(CPPFLAGS) *.c *.cpp > .depend

tags: $(SOURCES) $(MAIN_SRC) $(HEADERS) config.h
	if ctags --version | grep -q Exuberant; then ctags -R; else touch tags; fi

ifneq ($(MAKECMDGOALS),clean)
-include .depend
endif
