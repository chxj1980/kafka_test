#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#define elog(x, fmt, ...) do { printf(fmt, __VA_ARGS__); exit(0);} while(0)
#define ERROR 1
char errstr[512];

static size_t message_count;
static size_t bytes_count;
static int active_threads;
char* tp;
char* broker;
void *work(void *p)
{
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
    rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (kafka == NULL)
    {
        elog(ERROR, "rd_kafka_new failed: %s", errstr);
    }

    if (rd_kafka_brokers_add(kafka, broker) == 0)
    {
        elog(ERROR, "rd_kafka_brokers_add failed: %s", broker);
    }
    rd_kafka_topic_t *topic = rd_kafka_topic_new(kafka, tp, NULL);
    __sync_fetch_and_add(&active_threads, 1);
    size_t i = (size_t)p;
    if (rd_kafka_consume_start(topic, i, RD_KAFKA_OFFSET_BEGINNING) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
    }

    while (1)
    {
        rd_kafka_poll(kafka, 0);
        rd_kafka_message_t *msg = rd_kafka_consume(topic, i, 1000);
        if (msg)
        {
            rd_kafka_resp_err_t err = msg->err;
            size_t len = msg->len;
            rd_kafka_message_destroy(msg);
            if (err == 0)
            {
                __sync_fetch_and_add(&message_count, 1);
                __sync_fetch_and_add(&bytes_count, len);
            }
            else if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
                break;
            }
            else
            {
                elog(ERROR, "kafka consumer error: %s", rd_kafka_err2str(msg->err));
            }
        }
        else
        {
            continue;
        }
    }
    __sync_fetch_and_add(&active_threads, -1);
    return 0;
}

static pthread_t threads[1024];
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        printf("%s broker topic\n", argv[0]);
	return 0;
    }
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
    rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (kafka == NULL)
    {
        elog(ERROR, "rd_kafka_new failed: %s", errstr);
    }

    broker = argv[1];
    tp = argv[2];
    if (rd_kafka_brokers_add(kafka, broker) == 0)
    {
        elog(ERROR, "rd_kafka_brokers_add failed: %s", broker);
    }


    rd_kafka_topic_t *topic = rd_kafka_topic_new(kafka, tp, NULL);
    const rd_kafka_metadata_t* meta;
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_metadata(kafka, 0, topic, &meta, 1000))
    {
        elog(ERROR, "rd_kafka_metadata failed: %s", rd_kafka_err2str(rd_kafka_last_error()));
    }

    int count = meta->topics[0].partition_cnt;
    printf("using %d threads\n", count);
    for (size_t i = 0; i < count; i++)
    {
        pthread_create(&threads[i], 0, work, (void *)i);
    }

    size_t last_message = 0;
    size_t last_bytes = 0;
    while(active_threads > 0)
    {
        sleep(1);
        size_t this_message = message_count;
        size_t this_bytes = bytes_count;
        printf("messages %ld, bytes %ld\n", this_message - last_message, this_bytes - last_bytes);
        last_message = this_message;
        last_bytes = this_bytes;
    }
    for (size_t i = 0; i < count; i++)
    {
        pthread_join(threads[i], 0);
    }
    return 0;
}
