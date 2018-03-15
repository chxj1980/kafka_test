CFLAGS = -O0 -g
CPPFLAGS = $(shell pkg-config --cflags rdkafka)
LDFLAGS = $(shell pkg-config --libs --static rdkafka)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $< $(CPPFLAGS)

default: test.o
	gcc $(CFLAGS) -o kafka_test test.o $(LDFLAGS)
