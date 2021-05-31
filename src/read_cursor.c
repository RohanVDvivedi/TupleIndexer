#include<read_cursor.h>

int lock_page_and_open_read_cursor(read_cursor* rc, uint32_t read_page_id, uint32_t record_id, const tuple_def* record_def, const data_access_methods* dam_p);

int open_read_cursor(read_cursor* rc, const void* read_page, uint32_t record_id, const tuple_def* record_def);

int seek_next_read_cursor(read_cursor* rc, const data_access_methods* dam_p);

const void* get_record_from_read_cursor(read_cursor* rc);

void close_read_cursor(read_cursor* rc, const data_access_methods* dam_p);