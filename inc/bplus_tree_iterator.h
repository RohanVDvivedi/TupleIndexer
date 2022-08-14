#ifndef BPLUS_TREE_ITERATOR_H
#define BPLUS_TREE_ITERATOR_H

#include<stdint.h>

#include<data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

// this iterator can only be used to reading leaf tuples of the b+ tree

typedef struct bplus_tree_iterator bplus_tree_iterator;
struct bplus_tree_iterator
{
	void* curr_page;

	// page_id of the current page
	uint64_t curr_page_id;

	// curr_tuple_index is the index of the tuple, that the iterator points
	// in the curr_page (a bplus_tree_leaf page)
	uint32_t curr_tuple_index;

	const bplus_tree_tuple_defs* bpttd_p;

	const data_access_methods* dam_p;
};

#define LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE UINT32_MAX

// if the current page is NULL then a lock is acquired by the iterator's get_new function
// curr_tuple_index if UINT32_MAX then the iterator will point to the last 
bplus_tree_iterator* get_new_bplus_tree_iterator(void* curr_page, uint64_t curr_page_id, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// it moves the cursor forward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// returns pointer to the current tuple that the cursor points to
// it returns NULL, if the page that it points to is empty (0 tuples), ** this must not happen in a bplus_tree
// or if the page being pointed to is a NULL_PAGE_ID
// the pointer to the tuple returned by this function is valid only until next_*, prev_* and delete_* functions are not called
const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// it moves the cursor backward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// check if an error occurred after a next_* or a prev_* call on a bplus_tree_iterator
int error_occurred_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// all locks held by the iterator will be released before, the iterator is destroyed/deleted
void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

#endif