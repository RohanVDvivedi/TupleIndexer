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
	WRITE_LOCKED
};

// for reading tuples inside the page you need a page cursor
typedef struct page_cursor page_cursor;
struct page_cursor
{
	// the page_list that this page cursor is meant to read
	page_list* pl_p

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

const void* find_in_page_list(const page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int insert_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int delete_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

// external merge sort goes here
int create_externally_sorted_page_list();

#endif