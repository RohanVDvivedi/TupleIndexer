#ifndef BPLUS_TREE_ITERATOR_H
#define BPLUS_TREE_ITERATOR_H

#include<stdint.h>

#include<persistent_page.h>
#include<opaque_data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

// this iterator can only be used to reading leaf tuples of the b+ tree

typedef struct bplus_tree_iterator bplus_tree_iterator;
struct bplus_tree_iterator
{
	locked_pages_stack lps;

	// curr_tuple_index is the index of the tuple, that the iterator points
	// in the curr_page (a bplus_tree_leaf page)
	uint32_t curr_tuple_index;

	// WRITE_LOCK or READ_LOCK, for the leaves
	// all interior pages in case of a stacked_iterator are only READ_LOCK-ed
	int leaf_lock_type;

	const bplus_tree_tuple_defs* bpttd_p;

	const data_access_methods* dam_p;
};

#define LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE UINT32_MAX

// if the lps has its leaf page locked in write_mode, then the iterator will lock all following leaf pages in WRITE_LOCK
// if the size of the lps is > 1, then the iterator becomes stacked iterator, locking leaf pages using parent pages instead of the next and previous linkages
// the capacity of lps is expected to be atleast 2
// curr_tuple_index if UINT32_MAX then the iterator will point to the last
// after the call to this function, the lps is solely owned by the bplus_tree_iterator, on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
// and lps gets deinitialized by the bplus_tree_iterator only after delete_bplus_tree_iterator() call
bplus_tree_iterator* get_new_bplus_tree_iterator(locked_pages_stack lps, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// it moves the cursor forward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// returns pointer to the current tuple that the cursor points to
// it returns NULL, if the page that it points to is empty (0 tuples), ** this must not happen in a bplus_tree
// or if the page being pointed to is a NULL_PAGE_ID
// the pointer to the tuple returned by this function is valid only until next_*, prev_* and delete_* functions are not called
const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// it moves the cursor backward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// all locks held by the iterator will be released before, the iterator is destroyed/deleted
void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

#endif