#ifndef READ_CURSOR_H
#define READ_CURSOR_H

#include<stdint.h>

#include<tuple_def.h>

#include<data_access_methods.h>

// this read cursor is used to read either of page_list or bplus_tree

typedef struct read_cursor read_cursor;
struct read_cursor
{
	// the page that is locked for reading
	void* read_page;

	// definition of each record on the page
	tuple_def const * record_def;

	// index of the record that will be read
	uint32_t record_id;
};

// call open_read_cursor, only while read_page_id is read locked
int lock_page_and_open_read_cursor(read_cursor* rc, uint32_t read_page_id, uint32_t record_id, const tuple_def* record_def, const data_access_methods* dam_p);
int open_read_cursor(read_cursor* rc, void* read_page, uint32_t record_id, const tuple_def* record_def);

// seek returns 1, if the seek was success full
// it fails with 0, if we reached the end of b+tree
int seek_next_read_cursor(read_cursor* rc, const data_access_methods* dam_p);

// returns pointer to the record that the read cursor is pointing to
const void* get_record_from_read_cursor(read_cursor* rc, const data_access_methods* dam_p);

// you must call this method to release the locked page by the read cursor
void close_read_cursor(read_cursor* rc, const data_access_methods* dam_p);

#endif