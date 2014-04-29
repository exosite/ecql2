
CFLAGS=-I/usr/lib/erlang/usr/include/ -Wall -O3

all: ecql_decoding.so

ecql_decoding.so: ecql_decoding.c
	$(CC) $(CFLAGS) -o $@ -fpic -shared $<
