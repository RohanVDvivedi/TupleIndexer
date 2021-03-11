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

const void* find_in_page_list(const page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int insert_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int delete_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

// external merge sort goes here
int create_externally_sorted_page_list();

#endif