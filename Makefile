SOURCES=$(wildcard *.c)
HEADERS=$(wildcard *.h)
OBJECTS=$(SOURCES:.c=.o)

.PHONY: clean

%.o: %.c
	gcc -c $< -O2 $(CFLAGS)

toilet: $(OBJECTS)
	gcc -o toilet $(OBJECTS)

clean:
	rm -f toilet *.o .depend

.depend: $(SOURCES)
	gcc -MM *.c > .depend

-include .depend
