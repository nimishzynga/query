#ifndef __JSON_PARSER__
#define __JSON_PARSER__

#include <list>
#include <queue>


#define false 0
#define true 1

typedef enum {
    START,
    ROW_COUNT_TOKEN,
    ROW_COUNT,
    ROWS_TOKEN,
    ROWS_START,
    ROWS_FINISH,
    FINISH
} parser_state_t;

typedef enum {
    SORTER_ROW,
    SORTER_ROW_COUNT,
    SORTER_ROW_END
} sorter_state_t;

typedef struct {
    char *buffer; // should this be std::string
    int len;
} size_buffer;

typedef struct {
    sorter_state_t sorter_state;
    size_buffer row_data[3];
} row_t;

typedef enum {
    ID,
    KEY,
    VALUE
} buffer_type;

struct {
    const char *data;
    buffer_type bt;
} emitted_value[3] = {
    {"key", KEY},
    {"id", ID},
    {"value", VALUE}};

typedef std::list<row_t *> row_list_t;
typedef std::pair <row_t *, void *> row_pair;
class CompareRow {
    public:
        bool operator()(row_pair& t1, row_pair &t2)
        {
            if (t1.first->sorter_state == SORTER_ROW_COUNT) {
                return false;
            }
            if (t2.first->sorter_state == SORTER_ROW_COUNT) {
                return true;
            }
            size_buffer& firstKey = t1.first->row_data[KEY];
            size_buffer& secondKey = t2.first->row_data[KEY];
            int len = std::min(firstKey.len, secondKey.len);
            return strncmp(firstKey.buffer, secondKey.buffer, len) > 0;
        }
};

void parse_data_json(char *data, int len, row_list_t& row_list);
typedef std::priority_queue<row_pair , std::vector<row_pair>, CompareRow> priority_queue_row;
#endif
