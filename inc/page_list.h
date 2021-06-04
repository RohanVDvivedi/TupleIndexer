#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<rwlock.h>

#include<page_offset_util.h>
#include<read_cursor.h>
#include<page_list_writable_handle.h>

typedef struct page_list_handle page_list_handle;
struct page_list_handle
{
	// lock to protect head_id of the page list
	rwlock handle_lock;

	// page id of the head page of the page list
	uint32_t head_id;
};

int create_new_page_list(page_list_handle* plh);

// fails with 0, if the page_list is empty
int init_read_cursor(page_list_handle* plh, read_cursor* rc, const tuple_def* record_def, const data_access_methods* dam_p);

// fails with 0, if the page_list is empty
int init_writable_handle(page_list_handle* plh, page_list_writable_handle* plwh, const data_access_methods* dam_p);

#endif