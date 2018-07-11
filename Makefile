CFLAGS = -O0 -g
CPPFLAGS = $(shell pkg-config --cflags rdkafka)
LDFLAGS = $(shell pkg-config --libs --static rdkafka)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $< $(CPPFLAGS)

queue: worker_queue.o
	gcc $(CFLAGS) -o kafka_queue worker_queue.o $(LDFLAGS)

single: worker_single.o
	gcc $(CFLAGS) -o kafka_single worker_single.o $(LDFLAGS)

default: test.o
	gcc $(CFLAGS) -o kafka_test test.o $(LDFLAGS)
