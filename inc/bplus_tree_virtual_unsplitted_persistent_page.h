#define BPLUS_TREE_VIRTUAL_UNSPLITTED_PERSISTENT_PAGE_H
#define BPLUS_TREE_VIRTUAL_UNSPLITTED_PERSISTENT_PAGE_H

/*
**	This struct is just built to facilitate calculation of tuples that go in both the pages during a split
**	It must not be used beyond that usage
**  It eases calculation, by trying to provide an interface to a pesistent_page, as if it already includes the tuple that is to be inserted
**	This struct is just a wrapper to make your split calculation easy
*/

#include<persistent_page_functions.h>
#include<tuple_def.h>

typedef struct virtual_unsplitted_persistent_page virtual_unsplitted_persistent_page;
struct virtual_unsplitted_persistent_page
{
	// page to be split for an insert
	const persistent_page* ppage;

	// tuple that is to be inserted
	const void* tuple_to_insert;

	// the index at which in ppage, tuple_to_insert has to be inserted
	uint32_t insertion_index;

	// the tuple_definition to use for the ppage, and the tuple_to_insert
	const tuple_def* tpl_d;

	// above attributes are for get_nth_tuple, get_space_occupied_by_tuples and get_tuple_count for the persistent_page
	// below attributes along with the tpl_d are used for the contexted_comparator, to compare tuples on the page

	// element_ids of the tpl_d, that are key to this bplus_tree's (sorted_packed_page) page
	const uint32_t* key_element_ids;

	// the sort direction of the keys
	const compare_direction* key_compare_direction;

	// number of elements in the key_element_ids
	uint32_t key_element_count;
};

virtual_unsplitted_persistent_page get_virtual_unsplitted_persistent_page(const persistent_page* ppage, const void* tuple_to_insert, uint32_t insertion_index, const tuple_def* tpl_d, const uint32_t* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count);

uint32_t get_tuple_count_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p);

const void* get_nth_tuple_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t index);

uint32_t get_space_occupied_by_tuples_on_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t start_index, uint32_t last_index);

/*
**	FUTURE :: You will also need to add 
**	get_index_accessed_interface_for_virtual_unsplitted_persistent_page
**	comparator_function_for_virtual_unsplitted_persistent_page that is to be used like this ()
*/

#endif
