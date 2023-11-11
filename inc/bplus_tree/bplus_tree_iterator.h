#ifndef BPLUS_TREE_ITERATOR_H
#define BPLUS_TREE_ITERATOR_H

#include<stdint.h>

#include<locked_pages_stack.h>
#include<persistent_page.h>
#include<opaque_data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

// this iterator can only be used to reading leaf tuples of the b+tree

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

	// if this attribute is 1, then the iteration occurrs using the parent pages, else it happens through the next and prev page pointers on the leaf page
	int is_stacked;

	const bplus_tree_tuple_defs* bpttd_p;

	const data_access_methods* dam_p;
};

#define LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE UINT32_MAX

// if the lps has its leaf page locked in write_mode, then the iterator will lock all following leaf pages in WRITE_LOCK
// if the size of the lps is > 1, then the iterator becomes stacked iterator, locking leaf pages using parent pages instead of the next and previous linkages
// the capacity of lps is expected to be atleast 2
// curr_tuple_index if LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE then the iterator will point to the last
// after the successfull call to this function (return value != NULL), the lps is solely owned by the bplus_tree_iterator
// and lps gets deinitialized by the bplus_tree_iterator only after delete_bplus_tree_iterator() call
bplus_tree_iterator* get_new_bplus_tree_iterator(locked_pages_stack lps, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#include<bplus_tree_iterator.h>

#endif