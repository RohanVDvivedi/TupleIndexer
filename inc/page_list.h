#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<rwlock.h>

#include<read_cursor.h>

typedef struct page_list_handle page_list_handle;
struct page_list_handle
{
	// lock to protect head_id of the page list
	rwlock handle_lock;

	// page id of the head page of the page list
	uint32_t head_id;
};

#include<page_list_writable_handle.h>

int create_new_page_list(page_list_handle* plh);

int init_read_cursor(page_list_handle* plh, read_cursor* rc);

int init_writable_handle(page_list_handle* plh, page_list_writable_handle* plwh);

#endif