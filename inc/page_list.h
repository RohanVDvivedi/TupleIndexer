#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>

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
	uint32_t page_id;		// page_id of the page

	// the tuple that this page_cursor is pointing to at this instance
	void* tuple;
	uint16_t tuple_index;	// tuple_index of the tuple in the page

	// the definition of each tuple that this page_cursor may point to
	const tuple_def* tpl_d;

	// the data_access_methods for access data pages for this page_cursor
	const data_access_methods* dam_p;
};

// creates a new page_list and returns the id to head page of the the newly created page_list
uint32_t create_new_page_list(const data_access_methods* dam_p);

// page_cursor functions

// all the page cursor functions return 0 if end of page_list is reached

int initialize_cursor(page_cursor* pc_p, page_cursor_lock_type lock_type, page_cursor_traversal_direction traverse_dir, uint32_t page_list_page_id, const tuple_def* tpl_d, const data_access_methods* dam_p);

// page_cursor points to the next tuple in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int seek_cursor_to_next_tuple(page_cursor* pc_p);

// page_cursor points to the first tuple in the next page in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int seek_cursor_to_next_page_first_tuple(page_cursor* pc_p);

// page_cursor points to the first tuple in the current page in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int seek_cursor_to_current_page_first_tuple(page_cursor* pc_p);

// returns 1, if the page was split at the cursor
// splits page only if the tuple count >= 2
// the new page will be added to the next or prev of the current page (according to the traversal_dir of the cursor)
int split_page_at_cursor(page_cursor* pc_p);

// returns 1, if the page at the cursor was merged with the next or previous page (according to the traversal_dir of the cursor)
// the merge succeeds, only if the data of the both the pages can be accomodated in one page
int merge_page_at_cursor(page_cursor* pc_p);

// the tuple is inserted after the cursor if the flag "after_cursor" is set
// no seek is performed (the number of tuples already seeked remains the same)
// returns 0 => failure, 1 => succcess w/o page split, 2 => success with page split
int insert_tuple_at_cursor(page_cursor* pc_p, int after_cursor, const void* tuple_to_insert);

// no seek is performed (the number of tuples already seeked remains the same)
// returns 0 => failure, 1 => succcess w/o page merge, 2 => success with page merge
int delete_tuple_at_cursor(page_cursor* pc_p);

void deinitialize_cursor(page_cursor* pc_p);

// print page list

void print_page_list(uint32_t page_list_head_page_id, const data_access_methods* dam_p);

// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif

/*
	GROUND RULES

	* no page can have 0 tuple count, except if the page is the head page with
	NEXT_PAGE_REFERENCE_INDEX and PREV_PAGE_REFERENCE_INDEX both being NULL_PAGE_REFERENCE

	* upon insertion if the tuple can not accomodated, then the page is split in to two parts

	* upon deletion, a page is removed from the page_list only if it has 0 keys
*/