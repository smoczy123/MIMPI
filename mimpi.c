/**
 * This file is for implementation of MIMPI library.
 * */
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define START_FD 20
#define START_GROUP_FD 500

struct metadata {
    int count;
    int tag;
};

typedef struct metadata metadata_t;

struct node {
    void *data;
    int tag;
    int count;
    int bytes_read;
    struct node *next;
    struct node *prev;
};

typedef struct node node_t;

struct queue {
    node_t* head;
    node_t* tail;

};

typedef struct queue queue_t;

queue_t* new_queue() {
    queue_t* ret_val = malloc(sizeof(queue_t));
    node_t* head_guard = malloc(sizeof(node_t));
    node_t* tail_guard = malloc(sizeof(node_t));
    head_guard->count = INT_MIN;
    head_guard->tag = INT_MIN;
    head_guard->data = NULL;
    head_guard->bytes_read = 0;
    head_guard->prev = NULL;
    head_guard->next = tail_guard;
    tail_guard->count = INT_MIN;
    tail_guard->tag = INT_MIN;
    head_guard->bytes_read = 0;
    tail_guard->data = NULL;
    tail_guard->prev = head_guard;
    tail_guard->next = NULL;
    ret_val->head = head_guard;
    ret_val->tail = tail_guard;
    return ret_val;
}

void add_node(queue_t* queue, void* data, int count, int tag) {
    node_t* new_node = malloc(sizeof(node_t));
    new_node->data = data;
    new_node->tag = tag;
    new_node->count = count;
    new_node->bytes_read = 0;
    new_node->next = queue->tail;
    new_node->prev = queue->tail->prev;
    queue->tail->prev->next = new_node;
    queue->tail->prev = new_node;
}

void remove_node(node_t* node) {
    free(node->data);
    node->prev->next = node->next;
    node->next->prev = node->prev;
    free(node);
}

static int size;
static int rank;
static bool finished[16];
static queue_t* queues[16];
static pthread_t threads[16];
static pthread_mutex_t queue_mutex[16];
static pthread_cond_t queue_cond[16];


static void* worker_receiver(void *data) {
    int from = *(int*)data;
    free(data);
    int read_fd = determine_read(rank, from);
    //printf("from: %d read_fd: %d \n", from, read_fd);
    //print_open_descriptors();

    while(true) {
        metadata_t* metadata = malloc(sizeof(metadata_t));
        int bytes_read;
        int bytes_left = sizeof(metadata_t);
        while (bytes_left != 0) {
            ASSERT_SYS_OK(bytes_read = chrecv(read_fd, metadata + sizeof(metadata_t) - bytes_left, bytes_left));
            if (bytes_read == 0) {
                free(metadata);
                finished[from] = true;
                pthread_cond_signal(&queue_cond[from]);
                return NULL;
            }
            bytes_left -= bytes_read;
        }
        int count = metadata->count;
        bytes_left = count;
        int tag = metadata->tag;
        free(metadata);
        int msg_size = min(512, bytes_left);
        //printf("reading metadata count: %d, tag: %d\n", count, tag);
        void* read_data = malloc(count);

        while (bytes_left != 0) {
            ASSERT_SYS_OK(bytes_read = chrecv(read_fd, read_data + count - bytes_left, msg_size));
            if (bytes_read == 0) {
                free(read_data);
                ASSERT_ZERO(pthread_mutex_lock(&queue_mutex[from]));
                finished[from] = true;
                pthread_cond_signal(&queue_cond[from]);
                ASSERT_ZERO(pthread_mutex_unlock(&queue_mutex[from]));
                return NULL;
            }
            bytes_left -= bytes_read;
        }
        ASSERT_ZERO(pthread_mutex_lock(&queue_mutex[from]));
        add_node(queues[from], read_data, count, tag);
        ASSERT_ZERO(pthread_mutex_unlock(&queue_mutex[from]));
        pthread_cond_signal(&queue_cond[from]);
    }


}



void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    ASSERT_SYS_OK(size = strtol(getenv("MIMPI_SIZE"), NULL, 0));
    ASSERT_SYS_OK(rank = strtol(getenv("MIMPI_RANK"), NULL, 0));
    for (int i = 0; i < size; ++i) {
        if (i != rank) {
            finished[i] = false;
            queues[i] = new_queue();
            ASSERT_ZERO(pthread_mutex_init(&queue_mutex[i], NULL));
            ASSERT_ZERO(pthread_cond_init(&queue_cond[i], NULL));
            int* num = malloc(sizeof(int));
            *num = i;
            ASSERT_ZERO(pthread_create(&threads[i], NULL, worker_receiver, num));
        }

    }


}

void MIMPI_Finalize() {
    for (int i = 0; i < size; ++i) {
        if (i != rank) {
            ASSERT_SYS_OK(close(determine_write(rank, i)));
        }
    }
    for (int i = 0; i < size; ++i) {
        if (i != rank) {
            ASSERT_ZERO(pthread_join(threads[i], NULL));
            ASSERT_ZERO(pthread_mutex_destroy(&queue_mutex[i]));
            ASSERT_ZERO(pthread_cond_destroy(&queue_cond[i]));
            ASSERT_SYS_OK(close(determine_read(rank, i)));
        }
    }
    channels_finalize();
}

int MIMPI_World_size() {
    return size;
}

int MIMPI_World_rank() {
    return rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if (destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (destination >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    int send_fd = determine_write(rank, destination);

    metadata_t* meta = malloc(sizeof(metadata_t));
    meta->count = count;
    meta->tag = tag;
    int bytes_to_send = sizeof(metadata_t);
    int sent_bytes;
    //printf("sending metadata count: %d, tag: %d\n", count, tag);
    while (bytes_to_send != 0) {
        ASSERT_SYS_OK(sent_bytes = chsend(send_fd, meta + sizeof(metadata_t) - bytes_to_send, sizeof(metadata_t)));
        if (errno == EPIPE) {
            free(meta);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        bytes_to_send -= sent_bytes;
    }
    free(meta);
    //printf("sending %d\n", *(short*)data);
    bytes_to_send = count;
    while (bytes_to_send != 0) {
        void* package = malloc(512);
        memcpy(package, data + count - bytes_to_send, min(bytes_to_send, 512));
        ASSERT_SYS_OK(sent_bytes = chsend(send_fd, package, min(bytes_to_send, 512)));
        if (errno == EPIPE) {
            free(package);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        bytes_to_send -= sent_bytes;
        free(package);

    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (source >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    int bytes_written = 0;
    ASSERT_ZERO(pthread_mutex_lock(&queue_mutex[source]));
    while (bytes_written != count) {

        node_t* node = queues[source]->head->next;
        while (node->next != NULL && bytes_written != count) {
            if (node->tag == tag || (node->tag > 0 && tag == 0)) {
                int how_many_bytes = min(count - bytes_written, node->count - node->bytes_read);
                memcpy(data + bytes_written, node->data + node->bytes_read, how_many_bytes);
                node->bytes_read += how_many_bytes;
                bytes_written += how_many_bytes;
            }
            node = node->next;
            if (node->prev->count == node->prev->bytes_read) {
                remove_node(node->prev);
            }
        }
        if (bytes_written != count) {
            if (finished[source]) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            pthread_cond_wait(&queue_cond[source], &queue_mutex[source]);
        }
    }
    ASSERT_ZERO(pthread_mutex_unlock(&queue_mutex[source]));
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}