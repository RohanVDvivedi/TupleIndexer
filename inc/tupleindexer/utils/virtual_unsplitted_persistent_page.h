#ifndef VIRTUAL_UNSPLITTED_PERSISTENT_PAGE_H
#define VIRTUAL_UNSPLITTED_PERSISTENT_PAGE_H

/*
**	This struct is just built to facilitate calculation of tuples that go in both the pages during a split
**	It must not be used beyond that usage
**  It eases calculation, by trying to provide an interface to a pesistent_page, as if it already includes the tuple that is to be inserted
**	This struct is just a wrapper to make your split calculation easy
**	This struct is not a substitute for persistent_page
*/

#include<tupleindexer/utils/persistent_page_functions.h>
#include<tuplestore/tuple.h>

typedef struct virtual_unsplitted_persistent_page virtual_unsplitted_persistent_page;
struct virtual_unsplitted_persistent_page
{
	// page to be split for an insert
	const persistent_page* ppage;

	// size of the persistent_page
	uint32_t page_size;

	// tuple that is to be inserted
	const void* tuple_to_insert;

	// the index at which in ppage, tuple_to_insert has to be inserted
	uint32_t insertion_index;

	// the tuple_definition to use for the ppage, and the tuple_to_insert
	// bpttd->record_def for leaf pages and bpttd_p->index_def for interior pages
	const tuple_def* tpl_d;
};

virtual_unsplitted_persistent_page get_virtual_unsplitted_persistent_page(const persistent_page* ppage, uint32_t page_size, const void* tuple_to_insert, uint32_t insertion_index, const tuple_def* tpl_d);

uint32_t get_tuple_count_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p);

const void* get_nth_tuple_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t index);

// this can be higher than the get_space_allotted_to_all_tuples_on_persistent_page, for a given persistent_page
// because this is a virtual page, remember!!
uint32_t get_space_occupied_by_tuples_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t start_index, uint32_t last_index);

#endif
