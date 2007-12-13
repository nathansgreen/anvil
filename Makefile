SOURCES=$(wildcard *.c)
HEADERS=$(wildcard *.h)
OBJECTS=$(SOURCES:.c=.o)

.PHONY: all clean

all: tags toilet

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

toilet: $(OBJECTS)
	gcc -o toilet $(OBJECTS)

clean:
	rm -f toilet *.o .depend tags

.depend: $(SOURCES)
	gcc -MM *.c > .depend

tags: $(SOURCES) $(HEADERS)
	ctags -R

-include .depend
