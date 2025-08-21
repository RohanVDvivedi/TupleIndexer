#ifndef HEAP_PAGE_H
#define HEAP_PAGE_H

#include<tupleindexer/common/page_access_specification.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<tupleindexer/common/invalid_tuple_indices.h>

/*
	rules for using a heap_page

	* use only the provided functions here to modify the page
*/

// the below function returns a is_persistent_page_NULL(), only on an error
// if the error is not an abort_error, then it is just impossible to create a heap_page out of a database page, which is only possible for very small page sizes
persistent_page get_new_heap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// your tuple should occupy lesser amount of space on the page then this value, to easily fit on the page
// this is the value that goes inside the heap_table's unused_space, it is a rough estimate of what maximum sized append (not an update) can this page survive
// it is literally is space_allotted_to_all_tuples - space_occupied_by_all_tuples OR more likely free_space_on_page + fragmented_space_on_page
uint32_t get_unused_space_on_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d);

// returns true if the tombstones of the heap_page == tuple_count on the heap_page
int is_heap_page_empty(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d);

// returns the index on the page where tuple was inserted, it looks for an index starting from (*possible_insertion_index)
// indices of the existing tuple remain unchanged, keeping passing the same possible_insertion_index, for efficient insertions on the single page
// you will also need to initialize (*possible_insertion_index) = 0, for the first call
// if the insertion fails for any reason, INVALID_TUPLE_INDEX will be returned
uint32_t insert_in_heap_page(persistent_page* ppage, const void* tuple, uint32_t* possible_insertion_index, const tuple_def* tpl_d, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d);

#endif