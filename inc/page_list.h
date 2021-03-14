#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>

#include<rwlock.h>

#include<data_access_methods.h>

#define NULL_PAGE_REFERENCE (~0U)

#define NEXT_PAGE_REFERENCE_INDEX  0
//#define PREV_PAGE_REFERENCE_INDEX  1

// for reading tuples inside the page_list you need a page cursor
typedef struct page_cursor page_cursor;
struct page_cursor
{
	// the page of the page_list that this page_cursor is reading
	// this is the page that this cursor has locked for the lock_type (as READER or WRITER)
	void* page;
	uint32_t page_id;		// page_id of the page

	// the tuple that this page_cursor is pointing to at this instance
	void* tuple;
	uint16_t tuple_index;	// tuple_index of the tuple in the page
};

// creates a new page_list and returns the id to head page of the the newly created page_list
uint32_t get_new_page_list(const data_access_methods* dam_p);

// page_cursor functions

void initialize_cursor(page_cursor* pc_p, uint32_t page_list_page_id, const data_access_methods* dam_p);

// page_cursor points to the next tuple in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int seek_cursor_to_next_tuple(page_cursor* pc_p, const data_access_methods* dam_p);

// if deleted successfully, page_cursor points to the next tuple in the page_list, and returns 1
// else it returns 0 (if the end of page_list is reached)
int delete_tuple_at_the_cursor(page_cursor* pc_p, const void* tuple_like, const data_access_methods* dam_p);

// if deleted successfully, page_cursor points to the newlu inserted tuple in the page_list, and returns 1
// else it returns 0 (if the end of page_list is reached)
int insert_tuple_after_the_cursor(page_cursor* pc_p, const void* tuple_like, const data_access_methods* dam_p);

void deinitialize_cursor(page_cursor* pc_p);

// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif