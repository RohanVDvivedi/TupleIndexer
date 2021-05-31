#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<rwlock.h>

typedef struct page_list page_list;
struct page_list
{
	// lock to protect head_id of the page list
	rwlock handle_lock;

	// page id of the head page of the page list
	uint32_t head_id;
};

#endif