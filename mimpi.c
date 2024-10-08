/**
 * This file is for implementation of MIMPI library.
 * */
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"


struct metadata {
    int count;
    int tag;
};

typedef struct metadata metadata_t;

struct node {
    void *data;
    int tag;
    int count;
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
    head_guard->prev = NULL;
    head_guard->next = tail_guard;
    tail_guard->count = INT_MIN;
    tail_guard->tag = INT_MIN;
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

void delete_queue(queue_t* queue) {
    while (queue->head->next->next != NULL) {
        remove_node(queue->head->next);
    }
    free(queue->tail);
    free(queue->head);
    free(queue);
}

static void perform_op(void* src1, void* src2, int count, MIMPI_Op op) {
    u_int8_t* isrc1 = (u_int8_t*)src1;
    u_int8_t* isrc2 = (u_int8_t*)src2;
    for (int i = 0; i < count; ++i) {
        switch (op) {
            case MIMPI_MAX:
                isrc1[i] = max(isrc1[i], isrc2[i]);
                break;
            case MIMPI_MIN:
                isrc1[i] = min(isrc1[i], isrc2[i]);
                break;
            case MIMPI_SUM:
                isrc1[i] = isrc1[i] + isrc2[i];
                break;
            case MIMPI_PROD:
                isrc1[i] = isrc1[i] * isrc2[i];
                break;
        }
    }
}





static MIMPI_Retcode send_data_fn(int send_fd, int count, void* data) {
    int sent_bytes;
    int bytes_to_send = count;
    while (bytes_to_send != 0) {
        void* package = malloc(512);
        memcpy(package, data + count - bytes_to_send, min(bytes_to_send, 512));
        sent_bytes = chsend(send_fd, package, min(bytes_to_send, 512));
        if (sent_bytes == -1) {
            if (errno == EPIPE) {
                free(package);
                return MIMPI_ERROR_REMOTE_FINISHED;
            } else {
                ASSERT_SYS_OK(-1);
            }
        }
        bytes_to_send -= sent_bytes;
        free(package);
    }
    return MIMPI_SUCCESS;
}

static MIMPI_Retcode read_data_fn(int read_fd, int count, void* data) {
    int bytes_left = count;
    int bytes_read;
    while (bytes_left != 0) {
        int msg_size = min(512, bytes_left);
        ASSERT_SYS_OK(bytes_read = chrecv(read_fd, data + count - bytes_left, msg_size));
        if (bytes_read == 0) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        bytes_left -= bytes_read;
    }
    return MIMPI_SUCCESS;
}


static int size;
static int rank;
static bool deadlock_detection;
static queue_t* deadlock_queues[16];
static pthread_mutex_t deadlock_mutex[16];
static bool finished[16];
static queue_t* queues[16];
static pthread_t threads[16];
static pthread_mutex_t queue_mutex[16];
static pthread_cond_t queue_cond[16];

static MIMPI_Retcode check_deadlock(int destination) {

    node_t* temp_node = queues[destination]->head;
    while (temp_node->next != NULL) {
        //printf("checking for deadlock\n");
        if (temp_node->tag == -1) {
            //printf("sussy baka\n");
            metadata_t* meta = temp_node->data;
            int count = meta->count;
            int tag = meta->tag;
            //printf("Waiting for count: %d, tag: %d from %d\n", count, tag, destination);
            bool found = false;
            node_t* temp_dnode = deadlock_queues[destination]->head;
            while (temp_dnode->next != NULL) {
                if (count == temp_dnode->count && ((tag == temp_dnode->tag) || (tag == 0 && temp_dnode > 0))) {
                    temp_node = temp_node->next;
                    remove_node(temp_node->prev);
                    temp_dnode = temp_dnode->next;
                    remove_node(temp_dnode->prev);
                    found = true;
                    break;
                }
                temp_dnode = temp_dnode->next;
            }
            if (!found) {
                return MIMPI_ERROR_DEADLOCK_DETECTED;
            }
        } else {
            temp_node = temp_node->next;
        }
    }
    //printf("all good\n");
    return MIMPI_SUCCESS;
}

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
        void* read_data = malloc(count);

        while (bytes_left != 0) {
            int msg_size = min(512, bytes_left);
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
    deadlock_detection = enable_deadlock_detection;
    ASSERT_SYS_OK(size = strtol(getenv("MIMPI_SIZE"), NULL, 0));
    ASSERT_SYS_OK(rank = strtol(getenv("MIMPI_RANK"), NULL, 0));
    for (int i = 0; i < size; ++i) {
        if (i != rank) {
            finished[i] = false;
            queues[i] = new_queue();
            if (deadlock_detection) {
                deadlock_queues[i] = new_queue();
                ASSERT_ZERO(pthread_mutex_init(&deadlock_mutex[i], NULL));
            }
            ASSERT_ZERO(pthread_mutex_init(&queue_mutex[i], NULL));
            ASSERT_ZERO(pthread_cond_init(&queue_cond[i], NULL));
            int* num = malloc(sizeof(int));
            *num = i;
            ASSERT_ZERO(pthread_create(&threads[i], NULL, worker_receiver, num));
        }

    }


}

void MIMPI_Finalize() {

    if (group_num(rank, MIMPI_Father) >= 0) {
        ASSERT_SYS_OK(close(determine_gwrite(MIMPI_Father)));
        ASSERT_SYS_OK(close(determine_gread(MIMPI_Father)));                }
    if (group_num(rank, MIMPI_Left) < size) {
        ASSERT_SYS_OK(close(determine_gwrite(MIMPI_Left)));
        ASSERT_SYS_OK(close(determine_gread(MIMPI_Left)));
    }
    if (group_num(rank, MIMPI_Right) < size) {
        ASSERT_SYS_OK(close(determine_gwrite(MIMPI_Right)));
        ASSERT_SYS_OK(close(determine_gread(MIMPI_Right)));
    }
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
            delete_queue(queues[i]);
            if (deadlock_detection) {
                delete_queue(deadlock_queues[i]);
                ASSERT_ZERO(pthread_mutex_destroy(&deadlock_mutex[i]));
            }
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
    while (bytes_to_send != 0) {
        ASSERT_SYS_OK(sent_bytes = chsend(send_fd, meta + sizeof(metadata_t) - bytes_to_send, sizeof(metadata_t)));
        if (errno == EPIPE) {
            free(meta);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        bytes_to_send -= sent_bytes;
    }
    free(meta);
    bytes_to_send = count;
    while (bytes_to_send != 0) {
        void* package = malloc(512);
        memcpy(package, data + count - bytes_to_send, min(bytes_to_send, 512));
        sent_bytes = chsend(send_fd, package, min(bytes_to_send, 512));
        if (errno == EPIPE) {
            free(package);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        bytes_to_send -= sent_bytes;
        free(package);
    }
    if (deadlock_detection) {
        int* trash = malloc(sizeof(int));
        add_node(deadlock_queues[destination], trash, count, tag);
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
    bool done = false;

    ASSERT_ZERO(pthread_mutex_lock(&queue_mutex[source]));
    node_t* node = queues[source]->head->next;
    bool first = true;
    while (!done) {
        while (node->next != NULL && !done) {
            if ((node->tag == tag || (node->tag > 0 && tag == MIMPI_ANY_TAG)) && node->count == count) {
                memcpy(data, node->data, count);
                node = node->next;
                remove_node(node->prev);
                done = true;
            } else {
                node = node->next;
            }
        }
        if (!done) {
            //printf("checking for deadlock\n");
            if (deadlock_detection) {
                if (first) {
                    //printf("sending deadlock message\n");
                    metadata_t* metadata = malloc(sizeof(metadata_t));
                    metadata->count = count;
                    metadata->tag = tag;
                    MIMPI_Send(metadata, sizeof(metadata_t), source, -1);
                    free(metadata);
                    //printf("deadlock message sent\n");
                }
                MIMPI_Retcode retcode = check_deadlock(source);
                if (retcode == MIMPI_ERROR_DEADLOCK_DETECTED) {
                    pthread_mutex_unlock(&queue_mutex[source]);
                    return MIMPI_ERROR_DEADLOCK_DETECTED;
                }
            }
            if (finished[source]) {
                pthread_mutex_unlock(&queue_mutex[source]);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            node = node->prev;
            pthread_cond_wait(&queue_cond[source], &queue_mutex[source]);
            first = false;
        }
    }
    ASSERT_ZERO(pthread_mutex_unlock(&queue_mutex[source]));
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    void *data = malloc(1);
    memset(data, 0, 1);
    if (group_num(rank, MIMPI_Left) < size) {
        if (read_data_fn(determine_gread(MIMPI_Left), 1, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (read_data_fn(determine_gread(MIMPI_Right), 1, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Father) >= 0) {
        if (send_data_fn(determine_gwrite(MIMPI_Father), 1, data)) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        if (read_data_fn(determine_gread(MIMPI_Father), 1, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Left) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Left), 1, data)) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Right), 1, data)) {
            free(data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    free(data);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    if (root >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    int root_path;
    int tmp = root + 1;
    while (tmp > 0 && tmp != group_num(rank, MIMPI_Left) + 1) {
        tmp /= 2;
    }
    if (tmp == group_num(rank, MIMPI_Left) + 1) {
        root_path = 0;
    } else {
        root_path = 1;
    }
    void* data_array[2];
    data_array[0] = malloc(count);
    data_array[1] = malloc(count);
    memset(data_array[0], 0, count);
    memset(data_array[1], 0, count);
    if (group_num(rank, MIMPI_Left) < size) {
        if (read_data_fn(determine_gread(MIMPI_Left), count, data_array[0]) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (read_data_fn(determine_gread(MIMPI_Right), count, data_array[1]) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    void* data_to_send;
    if (rank == root) {
        data_to_send = data;
    } else {
        data_to_send = data_array[root_path];
    }
    if (group_num(rank, MIMPI_Father) >= 0) {

        if (send_data_fn(determine_gwrite(MIMPI_Father), count, data_to_send) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (read_data_fn(determine_gread(MIMPI_Father), count, data_array[root_path]) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        //printf("received %d\n" ,*(short*)data_array[root_path]);

    }

    if (rank != root) {
        memcpy(data, data_array[root_path], count);
        data_to_send = data_array[root_path];
    } else {
        data_to_send = data;
    }


    if (group_num(rank, MIMPI_Left) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Left), count, data_to_send) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Right), count, data_to_send) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    free(data_array[0]);
    free(data_array[1]);
    return MIMPI_SUCCESS;

}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    if (root >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    void* data = malloc(count);
    memcpy(data, send_data, count);
    void* data_array[2];
    data_array[0] = malloc(count);
    memset(data_array[0], 0, count);
    data_array[1] = malloc(count);
    memset(data_array[1], 0, count);
    if (group_num(rank, MIMPI_Left) < size) {
        if (read_data_fn(determine_gread(MIMPI_Left), count, data_array[0]) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        perform_op(data, data_array[0], count, op);

    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (read_data_fn(determine_gread(MIMPI_Right), count, data_array[1]) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        perform_op(data, data_array[1], count, op);
    }

    if (group_num(rank, MIMPI_Father) >= 0) {
        if (send_data_fn(determine_gwrite(MIMPI_Father), count, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (read_data_fn(determine_gread(MIMPI_Father), count, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (rank == root) {
        memcpy(recv_data, data, count);
    }

    if (group_num(rank, MIMPI_Left) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Left), count, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (group_num(rank, MIMPI_Right) < size) {
        if (send_data_fn(determine_gwrite(MIMPI_Right), count, data) == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(data_array[0]);
            free(data_array[1]);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    free(data);
    free(data_array[0]);
    free(data_array[1]);
    return MIMPI_SUCCESS;
}