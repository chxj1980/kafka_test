
#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>

#define elog(x, fmt, ...) do { printf(fmt, __VA_ARGS__); exit(0);} while(0)
#define ERROR 1
char errstr[512];

int running = 1;
static size_t message_count;
static size_t bytes_count;
static int active_threads;
char* tp;
char* broker;

void msg_consume (rd_kafka_message_t *msg, void *opaque)
{
   rd_kafka_resp_err_t err = msg->err;
   size_t len = msg->len;
   if (err == 0)
   {
       message_count++;
       bytes_count+=len;
   }
   else if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
   {
       running = 0;
       return;
   }
   else
   {
       elog(ERROR, "kafka consumer error: %s", rd_kafka_err2str(msg->err));
   }
}

static int work(int partition) 
{
    int pid = fork();
    if (pid > 0) return pid;
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

    if (rd_kafka_consume_start(topic, partition, RD_KAFKA_OFFSET_BEGINNING) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
    }

    while(running)
    {
        rd_kafka_consume_callback(topic, partition, 1000, msg_consume, 0);
    }
    printf("consumed %lu messages %lu bytes from partition %d\n", message_count, bytes_count, partition);
    exit(0);
}

int get_partitions() {
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
    const rd_kafka_metadata_t* meta;
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_metadata(kafka, 0, topic, &meta, 1000))
    {
        elog(ERROR, "rd_kafka_metadata failed: %s", rd_kafka_err2str(rd_kafka_last_error()));
    }

    rd_kafka_topic_destroy(topic);
    rd_kafka_destroy(kafka);
    return meta->topics[0].partition_cnt;
}

static int partitions[1024];
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        printf("%s broker topic\n", argv[0]);
        return 0;
    }

    broker = argv[1];
    tp = argv[2];
    int count = get_partitions();
    printf("consuming %d partitions\n", count);
    for (int i = 0; i < count; i++)
    {
        partitions[i] = work(i);
    }

    for (int i = 0; i < count; i++)
    {
        int status;
        waitpid(partitions[i], &status, 0);
    }
    return 0;
}

