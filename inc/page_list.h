#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>

#include<rwlock.h>

#include<data_access_methods.h>

#define NULL_PAGE_REFERENCE (~0U)

#define NEXT_PAGE_REFERENCE_INDEX  0
#define PREV_PAGE_REFERENCE_INDEX  1

typedef struct page_list page_list;
struct page_list
{
	rwlock page_list_lock;

	// the head page of the page_list
	uint32_t head_page_id;

	// the head page of the page_list
	uint32_t tail_page_id;

	// the tuple definition of the data at each of the pages
	tuple_def* tuple_definition;
};

// page_list functions

int initialize_page_list(page_list* pl_p, uint32_t head_page_id, uint32_t tail_page_id, tuple_def* tpl_d);

int insert_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int deinitialize_page_list(page_list* pl_p);

// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(page_list* pl_p, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif