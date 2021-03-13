#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>

#include<data_access_methods.h>

#define NULL_PAGE_REFERENCE (~0U)

#define NEXT_PAGE_REFERENCE_INDEX  0
#define PREV_PAGE_REFERENCE_INDEX  1

typedef struct page_list page_list;
struct page_list
{
	// the head page of the page_list
	uint32_t head_page_id;

	// the tuple definition of the data at each of the pages
	tuple_def* tpl_d;
};

// for reading tuples inside the page you need a page cursor
typedef struct page_cursor page_cursor;
struct page_cursor
{
	// the page_list that this page cursor is meant to read
	page_list* pg_list_p;

	// the page of the page_list that this page_cursor is reading
	// this is the page that this cursor has locked for the lock_type (as READER or WRITER)
	void* page;
	uint32_t page_id;		// page_id of the page

	// the tuple that this page_cursor is pointing to at this instance
	void* tuple;
	uint16_t tuple_index;	// tuple_index of the tuple in the page
};

// page_list functions

int initialize_page_list(page_list* pl_p, uint32_t head_page_id, tuple_def* tpl_d);

int insert_at_end_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int deinitialize_page_list(page_list* pl_p);


// page_cursor functions

int initialize_cursor(page_cursor* pc_p, page_list* pl_p, const data_access_methods* dam_p);

// if deleted successfully, page_cursor points to the next tuple in the page_list, and returns 1
// else it returns 0 (if the end of page_list is reached)
int delete_tuple_at_the_cursor(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

// page_cursor points to the next tuple in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int seek_to_next_tuple_in_page_list(page_cursor* pc_p, const data_access_methods* dam_p);

int deinitialize_cursor(page_cursor* pc_p);


// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(page_list* pl_p, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif