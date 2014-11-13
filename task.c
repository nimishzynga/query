/// get a thread
// find and send the request on the socket
// waiting for socket
// read from socket and put in queue
// collect and create a partial result
#include <event.h>
#include <stdarg.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <resolv.h>
#include <errno.h>
#include <pthread.h>
#include  <sys/ioctl.h>
#import <fcntl.h>
#include <stdlib.h>
#include "http_parser.h"
#include "platform/platform.h"

typedef struct {
    char *key;
    char *data;
} row;

typedef struct {
} queue;

void enqueue(queue *q, row *r);
row *dequeue(queue *q);

typedef enum {
    success,
    failure,
} status_code;

typedef enum {
    task_send_request,
    task_read_socket,
    task_write_socket,
    task_create_result,
    task_free
} task_type;

typedef struct {
    char *url;
    char *server;
    int port;
    queue *q; // local list for this connection
    struct event_base *base;
} send_request;

typedef struct {
    send_request **sr;
    int len;
} main_task;

typedef struct {
    task_type type;
    void *task_data;
} task;

#define MAX_BUFF 1024
#define MAX_READ_BUFF 2048
typedef struct conn {
     int fd;
     task_type state;
     struct event event;
     char write_buffer[MAX_BUFF];
     int write_size;
     struct conn *next;
     queue *q;
     http_parser *parser;
} conn;

typedef struct thread_task{
    conn *c;
    struct thread_task *next;
} thread_task;

typedef struct {
    thread_task *td;
    thread_task *last;
    cb_cond_t full;
    cb_mutex_t queue_lock;
} thread_task_queue;

struct {
    struct event_base *base;
    int thread_num;
    unsigned int thread_id;
    thread_task_queue **tq;
    cb_thread_t *thread_ids;
} global_data;


#define DEBUG(format, ...) fprintf(stderr, format, __VA_ARGS__)
#define PANIC(msg)  {perror(msg); abort();}
typedef status_code (*task_callback)(void *arg);
typedef void (*thread_func)(void *);

status_code send_request_to_server(void *arg);
status_code read_data(void *arg);
status_code create_result(void *arg);

int setNonblocking(int fd)
{
    int flags;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

conn *get_connection(char *server, int port) {
    int sockfd;
    struct sockaddr_in dest;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 )
        PANIC("Socket");

    bzero(&dest, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port); /*default HTTP Server port */
    if (inet_addr(server, &dest.sin_addr.s_addr) == -1)
        PANIC(server);

    if (connect(sockfd, (struct sockaddr*)&dest, sizeof(dest)) != 0)
        PANIC("Connect");

    setNonblocking(sockfd);
    conn *c = (conn *)malloc(sizeof(conn));
    c->parser = malloc(sizeof(http_parser));
    http_parser_init(c->parser, HTTP_REQUEST);
    c->fd = sockfd;
    c->next = NULL;
    c->state = task_send_request;
    return c;
}

void dispatch_task(conn *c) {
    int thread_id = global_data.thread_id++ % global_data.thread_num;
    thread_task *tt = malloc(sizeof(thread_task));
    tt->c = c;
    thread_task_queue *tq = global_data.tq[thread_id];
    //fprintf(stderr, "thread is is %u", &tq->queue_lock);
    cb_mutex_enter(&tq->queue_lock);
    if (tq->td == NULL) {
        tq->td = tt;
        tq->last = tt;
    } else {
        tq->last->next = tt;
        tq->last = tt;
        tt->next = NULL;
    }
    cb_cond_signal(&tq->full);
    cb_mutex_exit(&tq->queue_lock);
}

thread_task *get_item(thread_task_queue *tq) {
    cb_mutex_enter(&tq->queue_lock);
    if (tq->td == NULL) {
        cb_cond_wait(&tq->full, &tq->queue_lock);
    }
    thread_task *ptr = tq->td;
    ptr->next = tq->td;
    if (tq->last == ptr) {
        tq->last = tq->td;
    }
    cb_mutex_exit(&tq->queue_lock);
    return ptr;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    conn *c = (conn *) arg;
    if (which == EV_READ) {
        printf ("libevent event is %d %d %d\n", which, EV_WRITE, EV_READ);
    }
    dispatch_task(c);
}

status_code send_request_to_server(void *arg) {
    send_request *sreq = (send_request *)arg;
    conn *c = get_connection(sreq->server, sreq->port);
    if (c == NULL) {
        // TODO:handle the error in connetion
    }
    c->q = sreq->q;
    c->write_size = sprintf(c->write_buffer, "GET %s HTTP/1.0\n\n", sreq->url);
    c->state = task_write_socket;
    event_set(&c->event, c->fd, EV_WRITE, event_handler, (void *)c);
    event_base_set(sreq->base, &c->event);
    event_add(&c->event, 0);
    return success;
}

void create_threads(int num, thread_func tf) {
    global_data.thread_ids = calloc(num, sizeof(cb_thread_t));
    global_data.tq = calloc(num, sizeof(thread_task_queue *));
    //fprintf(stderr, "creating thread %d", num);
    for (int i = 0; i < num; i++) {
        thread_task_queue *tq = calloc(1, sizeof(thread_task_queue));
        cb_mutex_initialize(&tq->queue_lock);
        cb_cond_initialize(&tq->full);
        global_data.tq[i] = tq;
        //fprintf(stderr, "xxx thread is is %u xx", &tq->queue_lock);
    }
    int ret;
    for (int i=0;i<num;i++) {
        if ((ret = cb_create_thread(&global_data.thread_ids[i],
                tf, global_data.tq[i], 0)) != 0) {
            exit(1);
        }
    }
}

/*void parse_streaming_json_and_queue(char *buf, queue *q) {
    while (row *r = get_row(buf) != NULL) {
        enqueue(q, r);
    }
}*/

int read_and_parse_data(int fd, queue *q) {
    char buf[MAX_READ_BUFF];
    int count = read(fd, buf, MAX_READ_BUFF);
    printf("task count is %d errno is %s\n", count, strerror(errno));
    if (count > 0) {
        printf("got data %s", buf);
    }
    return count;
    //parse_streaming_json_and_queue(buf, q);
}

void remove_conn_from_free_pool() {
}

// need to handle the error in updating the event
int update_event(conn *c, int flags) {
    if (event_del(&c->event) == -1) {
        return -1;
    }
    event_set(&c->event, c->fd, flags,
            event_handler, (void *)c);
    event_base_set(global_data.base, &c->event);
    event_add(&c->event, 0);
    return 0;
}

void handle_state(conn *c) {
    //printf("handling state with state %d\n", c->state);
    switch(c->state) {
        case task_write_socket:
            if (c->write_size == 0) {
                DEBUG("%s", "write is zero");
                return;
            }
            DEBUG("this is test");
            write(c->fd, c->write_buffer, c->write_size);
            c->state = task_read_socket;
            if (update_event(c, EV_READ | EV_PERSIST) == -1) {
                fprintf(stderr, "read event not added");
            }
            break;
        case task_read_socket:
            fprintf(stderr, "in reading socket");
            if (read_and_parse_data(c->fd, c->q) == -1) {
                fprintf(stderr, "in reading socket1");
                break;
            }
            c->state = task_free;
            break;
        case task_free:
            //event during the task free
            //close(c->fd);
            remove_conn_from_free_pool(c);
    }
}

void thread_worker(void *arg) {
    DEBUG("%s", "inside the thread workder");
    thread_task_queue *tq = (thread_task_queue *)arg;
    thread_task *tt;
    do {
        tt = get_item(tq);
        conn *c = tt->c;
        handle_state(c);
    } while (1);
}

void add_sort_task(main_task *mt) {
}

void add_tasks(main_task *mt) {
    for (int i = 0; i < mt->len; i++) {
        send_request_to_server(mt->sr[i]);
    }
    add_sort_task(mt);
}

void init_query() {
    global_data.base = event_base_new();
    create_threads(global_data.thread_num, thread_worker);
}

int run_event_loop() {
    return event_base_loop(global_data.base, 0);
}

#define URL "/default/_design/1/_view/1?stale=false"

int main() {
    global_data.thread_num = 1;
    init_query();
    send_request sq = {.url = URL,
        .server = "127.0.0.1",
        .port = 9500,
        .base = global_data.base
    };
    if (send_request_to_server(&sq) == success) {
        printf("got success");
    }
    printf("event loop returned %d", run_event_loop());
    DEBUG("%s", "exited");
}
