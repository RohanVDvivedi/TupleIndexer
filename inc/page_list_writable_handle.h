#ifndef PAGE_LIST_WRITABLE_HANDLE_H
#define PAGE_LIST_WRITABLE_HANDLE_H

#include<page_list.h>

// to write to the page list you need a writable_handle
// you effectively lock two pages of the list

typedef struct page_list_writable_handle page_list_writable_handle;
struct page_list_writable_handle
{
	// the page_list that this handle belongs to
	page_list_handle* plh;

	// set to 1 if the curr is the first page of the page_list
	// this indicates that prev must be NULL and the page_list_handle is locked
	int is_curr_the_first_page;

	// null if this is the first page in the page_list
	void* prev;

	// pointer to the current locked page in the page_list
	void* curr;
};

#endif