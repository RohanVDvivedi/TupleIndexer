#ifndef PAGE_LIST_H
#define PAGE_LIST_H

#include<stdint.h>

#include<tuple.h>

#include<data_access_methods.h>

typedef struct page_list page_list;
struct page_list
{
	// the head page of the page_list
	uint32_t head_page;

	// the tuple definition of the data at each of the pages
	const tuple_def* tpl_d;
};

typedef enum page_lock_type page_lock_type;
enum page_lock_type
{
	READ_LOCKED,
	WRITE_LOCKED,
};

// for reading tuples inside the page you need a page cursor
typedef struct page_cursor page_cursor;
struct page_cursor
{
	// the page_list that this page cursor is meant to read
	page_list* pg_list_p;

	// the page in side the page_list that this page_cursor is reading
	void* page;
	uint32_t page_id;		// page_id of the page

	// number of indirections made in the page_list to reach the current page
	// it is 0, if the page_id == pl_p->head_page
	uint32_t indirections;

	// the page lock type held on the page of the page list
	page_lock_type lock_type;

	// the tuple that this page_cursor is pointing to at this instane
	void* tuple;
	uint16_t tuple_index;	// tuple_index of the tuple in the page
};

// returns 0 if there are no new tuple in the page_list, else returns 1
int seek_to_next_tuple_in_page_list(page_cursor* pc_p, const data_access_methods* dam_p);

int insert_at_end_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

// page_cursor points to the next tuple in the page_list, and return 1
// else it returns 0 (if the end of page_list is reached)
int delete_tuple_at_the_cursor(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

// external merge sort goes here
// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(page_list* pl_p, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif