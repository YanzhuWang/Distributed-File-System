all: mkfs server lib

mkfs:
	gcc mkfs.c -o mkfs

server:
	gcc server.c udp.c -o server

lib: mfs.o udp.o
	gcc -Wall -Werror -shared -fPIC -g -o libmfs.so mfs.c udp.c

clean:
	rm *.so
	rm *.o
	rm server
	rm mkfs