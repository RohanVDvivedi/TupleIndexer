#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>
#include<tuple_def.h>
#include<page_layout.h>

#include<rwlock.h>

#include<data_access_methods.h>

#define NULL_PAGE_REFERENCE (~0U)

#define NEXT_PAGE_REFERENCE_INDEX  0
#define PREV_PAGE_REFERENCE_INDEX  1

typedef enum page_cursor_lock_type page_cursor_lock_type;
enum page_cursor_lock_type
{
	READER_LOCK,
	WRITER_LOCK,
};

typedef enum page_cursor_traversal_direction page_cursor_traversal_direction;
enum page_cursor_traversal_direction
{
	NEXT_PAGE_DIRECTION = 0, // = NEXT_PAGE_REFERENCE_INDEX
	PREV_PAGE_DIRECTION = 1, // = PREV_PAGE_REFERENCE_INDEX
};

// for reading tuples inside the page_list you need a page cursor
typedef struct page_cursor page_cursor;
struct page_cursor
{
	page_cursor_lock_type lock_type;

	page_cursor_traversal_direction traverse_dir;

	// the page of the page_list that this page_cursor is reading
	// this is the page that this cursor has locked for the lock_type (as READER or WRITER)
	void* page;
	uint32_t page_id;		// page_id of the locked page

	// the definition of each tuple that this page_contains
	const tuple_def* tpl_d;

	// the data_access_methods for access data pages for this page_cursor
	const data_access_methods* dam_p;
};

// creates a new page_list and returns the id to head page of the the newly created page_list
uint32_t create_new_page_list(const data_access_methods* dam_p);

void initialize_cursor(page_cursor* pc_p, page_cursor_lock_type lock_type, page_cursor_traversal_direction traverse_dir, uint32_t page_list_page_id, const tuple_def* tpl_d, const data_access_methods* dam_p);
void deinitialize_cursor(page_cursor* pc_p);

// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);

// print page list
// TODO :: This function will be removed in the future
#include<in_memory_data_store.h>
void print_page_list_debug(uint32_t page_list_head_page_id, const memory_store_context* cntxt, uint32_t page_size, const tuple_def* tpl_d);

#endif