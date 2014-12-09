/// get a thread
// find and send the request on the socket
// waiting for socket
// read from socket and put in queue
// collect and create a partial result
#include <iostream>
using namespace std;
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
#include <netinet/in.h>
#include <arpa/inet.h>
#import <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include "http_parser.h"
#include "platform/platform.h"
#include "json_parse.h"
#include <queue>

typedef struct {
    char *key;
    char *data;
} row;

typedef struct {
} queue;

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
    struct event_base *base;
} send_request;

typedef struct {
    send_request *sr;
    int size;
} main_task_t;

typedef struct {
    task_type type;
    void *task_data;
} task;

#define MAX_BUFF 40240
#define MAX_READ_BUFF 2048
typedef struct conn {
     int fd;
     task_type state;
     struct event event;
     char write_buffer[MAX_BUFF];
     char json_buffer[MAX_BUFF];
     int write_size;
     int json_size;
     parser_data *parser;
     struct conn *next;
     short which;
     row_list_t query_row;
     cb_cond_t row_cond;
     cb_mutex_t row_mutex;
     bool query_end;
     void *data;
} conn;

typedef struct {
    conn **c;
    int size;
} sort_task_t;

typedef struct thread_task{
    conn *c;
    struct thread_task *next;
} thread_task;

typedef struct {
    thread_task *head;
    thread_task *last;
    cb_cond_t full;
    cb_mutex_t queue_lock;
    int item_count;
    int thread_id;
} thread_task_queue;

struct {
    struct event_base *base;
    int thread_num;
    unsigned int thread_id;
    thread_task_queue **tq;
    cb_thread_t *thread_ids;
} global_data;


#define DEBUG(format, ...) fprintf(stderr, format, __VA_ARGS__)
#define DEBUG_STR(str) fprintf(stderr, str)
#define PANIC(msg)  {perror(msg); abort();}
typedef status_code (*task_callback)(void *arg);
typedef void (*thread_func)(void *);
int parse_json(http_parser *parser, const char *at, size_t len);
int message_complete(http_parser *parser);
row_t *get_row(conn *);

conn *send_request_to_server(void *arg);
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
    dest.sin_addr.s_addr = inet_addr(server);

    if (connect(sockfd, (struct sockaddr*)&dest, sizeof(dest)) != 0)
        PANIC("Connect");

    setNonblocking(sockfd);
    conn *c = new conn();
    c->parser = init_http_parser(parse_json, message_complete, (void *) c);
    c->fd = sockfd;
    c->next = NULL;
    c->state = task_send_request;
    c->query_end = false;
    cb_cond_initialize(&c->row_cond);
    cb_mutex_initialize(&c->row_mutex);
    return c;
}

void dispatch_task(conn *c, int thread_id) {
    thread_task *tt = new thread_task();
    tt->c = c;
    thread_task_queue *tq = global_data.tq[thread_id];
    //fprintf(stderr, "thread is is %u", &tq->queue_lock);
    cb_mutex_enter(&tq->queue_lock);
    if (tq->head == NULL) {
        tq->head = tt;
        tq->last = tt;
    } else {
        tq->last->next = tt;
        tq->last = tt;
        tt->next = NULL;
    }
    tq->item_count++;
    DEBUG("queue item are %d %u state %d\n", tq->item_count, thread_id, c->state);
    cb_cond_signal(&tq->full);
    cb_mutex_exit(&tq->queue_lock);
}

//rename td to head
void dispatch_task(conn *c) {
    int thread_id = global_data.thread_id++ % global_data.thread_num;
    dispatch_task(c, thread_id);
}

void dispatch_sort_task(conn *c) {
    dispatch_task(c, global_data.thread_num);
}

thread_task *get_item(thread_task_queue *tq) {
    cb_mutex_enter(&tq->queue_lock);
    if (tq->head == NULL) {
        DEBUG("thread is waiting %d\n", tq->thread_id);
        cb_cond_wait(&tq->full, &tq->queue_lock);
        DEBUG("thread is free %d\n", tq->thread_id);
    }
    thread_task *ptr = tq->head;
    tq->head = ptr->next;
    if (tq->last == ptr) {
        tq->last = tq->head;
    }
    tq->item_count--;
    cb_mutex_exit(&tq->queue_lock);
    return ptr;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    conn *c = (conn *) arg;
    c->which = which;
    DEBUG("event handler called with %d %d\n", which, EV_WRITE);
    dispatch_task(c);
}

conn *send_request_to_server(void *arg) {
    send_request *sreq = (send_request *)arg;
    conn *c = get_connection(sreq->server, sreq->port);
    if (c == NULL) {
        // TODO:handle the error in connetion
    }
    c->write_size = sprintf(c->write_buffer, "GET %s HTTP/1.1\n\n", sreq->url);
    c->state = task_write_socket;
    event_set(&c->event, c->fd, EV_WRITE, event_handler, (void *)c);
    event_base_set(sreq->base, &c->event);
    event_add(&c->event, 0);
    return c;
}

void create_threads(int num, thread_func tf) {
    global_data.thread_ids = new cb_thread_t[num]();
    global_data.tq = new thread_task_queue*[num]();
    //fprintf(stderr, "creating thread %d", num);
    for (int i = 0; i < num; i++) {
        thread_task_queue *tq = new thread_task_queue();
        tq->thread_id = i;
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

int parse_json(http_parser *parser, const char *at, size_t len) {
    DEBUG_STR("parse json is called \n");
    conn *c = (conn *) parser->data;
    memcpy(&c->json_buffer[c->json_size], at, len);
    c->json_size += len;
    //printf("got the body at %s with length %d", at, len);
    return 0;
}

int message_complete(http_parser *parser) {
    conn *c = (conn *) parser->data;
    //DEBUG("total buffer is %.*s\n", c->json_size, c->json_buffer);
    DEBUG_STR("message complete");
    parse_data_json(c->json_buffer, c->json_size, c->query_row);
    DEBUG("size after message complete %d\n", c->query_row.size());
    c->json_size = 0;
    c->state = task_free;
    cb_cond_signal(&c->row_cond);
    return 0;
    // need to call the json parser
}

//need to create a task for this
void parse_streaming_json_and_queue(sort_task_t *st) {
    conn **c = st->c;
    int len = st->size;
    delete st;
    DEBUG_STR("inside parsing stream json and queue \n");
    priority_queue_row pq;
    queue<conn *> conn_vector;
    for (int i = 0; i < len; i++) {
        conn_vector.push(c[i]);
    }
    while (1) {
        while (!conn_vector.empty()) {
            conn *cv = conn_vector.front();
            conn_vector.pop();
            row_t *p = get_row(cv);
            if (p->sorter_state != SORTER_ROW_END) {
                row_pair rp = std::make_pair(p, cv);
                pq.push(rp);
            } else {
                DEBUG("going into sorter end state %u\n", cv);
            }
        }
        if (!pq.empty()) {
            row_pair rp = pq.top();
            row_t *rptr = rp.first;
            conn *cc = reinterpret_cast<conn *>(rp.second);
            conn_vector.push(cc);
            pq.pop();
            DEBUG("row is %.*s\n", rptr->row_data[KEY].len, rptr->row_data[KEY].buffer);
        } else {
            return;
        }
    }
}

row_t *get_row(conn *c) {
    DEBUG("getting row from %u\n", c);
    row_list_t& row_list = c->query_row;
    cb_mutex_enter(&c->row_mutex);
    if (c->query_end) {
        cb_mutex_exit(&c->row_mutex);
        return NULL;
    } else if (row_list.empty()) {
        cb_cond_wait(&c->row_cond, &c->row_mutex);
    }
    row_t *p = row_list.front();
    row_list.pop_front();
    if (p->sorter_state == SORTER_ROW_END) {
        c->query_end = true;
    }
    cb_mutex_exit(&c->row_mutex);
    return p;
}

int read_and_parse_data(conn *c, int fd) {
    char buf[MAX_READ_BUFF];
    int count = read(fd, buf, MAX_READ_BUFF);
    printf("task count is %d errno is %s\n", count, strerror(errno));
    errno = 0;
    if (count > 0) {
        parse_data(buf, count, c->parser);
    } else if (count == 0) {
        DEBUG_STR("connection closed \n");
    }
    return count;
    //parse_streaming_json_and_queue(buf, q);
}

void remove_conn_from_free_pool(conn *c) {
}

// need to handle the error in updating the event
int update_event(conn *c, int flags) {
    if (event_del(&c->event) == -1) {
        return -1;
    }
    event_set(&c->event, c->fd, flags,
            event_handler, (void *)c);
    event_base_set(global_data.base, &c->event);
    return event_add(&c->event, 0);
}

void handle_state(conn *c, int thread_id) {
    //printf("handling state with state %d\n", c->state);
    switch(c->state) {
        case task_write_socket:
            if (c->write_size == 0) {
                DEBUG("%s", "write is zero");
                return;
            }
            DEBUG_STR("this is test");
            write(c->fd, c->write_buffer, c->write_size);
            c->state = task_read_socket;
            if (update_event(c, EV_READ) == -1) {
                DEBUG_STR("read event not added");
            } else {
                DEBUG_STR("read event updated \n");
            }
            break;
        case task_read_socket:
            fprintf(stderr, "in reading socket");
            if (read_and_parse_data(c, c->fd) == -1) {
                DEBUG_STR("in reading socket1\n");
                DEBUG("error event %d\n", c->which);
            }
            update_event(c, EV_READ);
            break;
        case task_create_result:
            parse_streaming_json_and_queue((sort_task_t *) c->data);
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
        DEBUG("got the item in thread %d with item count %d\n", tq->thread_id, tq->item_count);
        conn *c = tt->c;
        handle_state(c, tq->thread_id);
        if (tq->thread_id == 0) {
            DEBUG_STR("handled state for thread id 0\n");
        }
    } while (1);
}

// this connection can be the client connection
void add_sort_task(conn **c, int size) {
    conn *cc = new conn();
    cc->state = task_create_result;
    sort_task_t *st = new sort_task_t();
    st->c = c;
    st->size = size;
    cc->data = (void *) st;
    dispatch_sort_task(cc);
}

void add_tasks(main_task_t *mt) {
    conn **cc = new conn*[mt->size]();
    for (int i = 0; i < mt->size; i++) {
        cc[i] = send_request_to_server(&mt->sr[i]);
    }
    add_sort_task(cc, mt->size);
}

void init_query() {
    global_data.base = event_base_new();
    create_threads(global_data.thread_num + 1, thread_worker);
}

int run_event_loop() {
    return event_base_loop(global_data.base, 0);
}

#define URL "/default/_design/1/_view/1?stale=false"

int main() {
    global_data.thread_num = 3;
    init_query();
    send_request sq[3] = {{.url = URL,
        .server = "127.0.0.1",
        .port = 9500,
        .base = global_data.base
    },{.url = URL,
        .server = "127.0.0.1",
        .port = 9500,
        .base = global_data.base
    },{.url = URL,
        .server = "127.0.0.1",
        .port = 9500,
        .base = global_data.base
    }};
#if 0
    send_request sq[1] = {{.url = URL,
        .server = "127.0.0.1",
        .port = 9500,
        .base = global_data.base
    }};
#endif
    main_task_t mt = {sq, 3};
    add_tasks(&mt);
#if 0
    if (send_request_to_server(&sq) != NULL) {
        printf("got success");
    }
#endif
    while (1) {
        int ret = run_event_loop();
        DEBUG("loop returned %d\n", ret);
    }
    DEBUG("%s", "exited");
}
