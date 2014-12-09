#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <list>
#include "jsmn.h"
#include "json_parse.h"

int str2int(const char* str, int len) {
    int i;
    int ret = 0;
    for(i = 0; i < len; ++i)
    {
        ret = ret * 10 + (str[i] - '0');
    }
    return ret;
}

// add a local queue pointer here to add the row to the queue
row_t *enque_data(jsmntok_t *t, int len, char *buff, row_list_t& row_list) {
    row_list_t back;
    parser_state_t ps = START;
    int row_count, size;
    int i = 0;
    int error = false;
    row_t *rptr = NULL;

    fprintf(stderr, "size of row_list is %d\n", row_list.size());
    for (;i < len; i++) {
        int offset = t[i].start;
        int length = t[i].end - t[i].start;
        switch(ps) {
            case START:
                ps = ROW_COUNT_TOKEN;
                break;
            case ROW_COUNT_TOKEN:
                if (strncmp(&buff[offset], "total_rows", sizeof("total_rows") - 1) != 0) {
                    printf("got error in parsing the total rows");
                    error = true;
                }
                ps = ROW_COUNT;
                break;
            case ROW_COUNT:
                row_count = str2int(&buff[offset], length);
                printf("row count is %d", row_count);
#if 0
                rptr = new row_t;
                rptr->sorter_state = SORTER_ROW_COUNT;
                row_list.push_back(rptr);
#endif
                ps = ROWS_TOKEN;
                break;
            case ROWS_TOKEN:
                if (strncmp(&buff[offset], "rows", sizeof("rows") - 1) != 0) {
                    printf("got error in parsing the rows token");
                    error = true;
                }
                ps = ROWS_START;
                break;
            case ROWS_START:
                size = t[i].size;
                if (t[i].type != JSMN_ARRAY) {
                    printf("error in parsing rows");
                    error = true;
                    break;
                }
                int j;
                i++;
                //rptr = calloc(size, sizeof(row_t));
                for(j = 0; j < size; j++) {
#define tok t[i]
                    rptr = new row_t;
                    if (tok.type != JSMN_OBJECT) {
                        printf("error in parsing rows array (%d) \n", tok.type);
                        error = true;
                        break;
                    } else {
                        i++;
                    }
                    if (strncmp(&buff[tok.start], "id", sizeof("id") - 1) == 0) {
                        i++;
                        rptr->row_data[KEY].buffer = &buff[t[i].start];
                        rptr->row_data[KEY].len= t[i].end - t[i].start;
                        i++;
                    }
                    if (strncmp(&buff[tok.start], "key", sizeof("key") - 1) == 0) {
                        i++;
                        rptr->row_data[ID].buffer = &buff[t[i].start];
                        rptr->row_data[ID].len= t[i].end - t[i].start;
                        i++;
                    }
                    if (strncmp(&buff[tok.start], "value", sizeof("value") - 1) == 0) {
                        i++;
                        rptr->row_data[VALUE].buffer = &buff[t[i].start];
                        rptr->row_data[VALUE].len= t[i].end - t[i].start;
                        i++;
                    }
                    row_list.push_back(rptr);
                }
                rptr = new row_t;
                rptr->sorter_state = SORTER_ROW_END;
                row_list.push_back(rptr);
            case ROWS_FINISH:
            case FINISH:
                return rptr;
        }
        if (error) {
            printf("error happend in parsing");
            return NULL;
        }
    }
}

void parse_data_json(char *data, int len, row_list_t& row_list) {
    jsmn_parser p;
    jsmntok_t t[10000];
    row_t *rptr;

    jsmn_init(&p);
    int r = jsmn_parse(&p, data, len, t, 10000);
    enque_data(t, r, data, row_list);
    fprintf(stderr, "size after parse_data_json %d with jsmn error %d \n", row_list.size(), r);
}

char *get_json(char *buff, int len) {
    FILE *fp;
    if ((fp=fopen("./sample_json", "r")) == NULL) {
        abort();
    }
    int len1 = fread((void *) buff, 1, len, fp);
    buff[len1] = 0;
    return buff;
}

#if 0
int main() {
    jsmn_parser p;
    jsmntok_t t[100];
    char buff[1000];
    row_t *rptr;
    row_list_t row_list;

    char *js = get_json(buff, 1000);
    printf("input is %s", js);
    jsmn_init(&p);
    int r = jsmn_parse(&p, js, strlen(js), t, 100);
    rptr = enque_data(t, r, buff, row_list);
    if (rptr == NULL) {
        printf("did not get the rows");
    } else {
        int i = 3, j = 0;
        row_list_t::iterator it;
        for (it=row_list.begin(); it!=row_list.end(); ++it) {
            rptr = *it;
            for (int i=0; i<3;i++) {
                printf("buffer is %.*s\n", rptr->row_data[i].len, rptr->row_data[i].buffer);
            }
        }
    }
}
#endif
