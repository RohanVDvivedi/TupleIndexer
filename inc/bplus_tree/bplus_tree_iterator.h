#ifndef BPLUS_TREE_ITERATOR_H
#define BPLUS_TREE_ITERATOR_H

#include<stdint.h>

#include<locked_pages_stack.h>
#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// this iterator can only be used to reading leaf tuples of the b+tree

typedef struct bplus_tree_iterator bplus_tree_iterator;
struct bplus_tree_iterator
{
	// if this attribute is 1, then the iteration occurrs using the parent pages, else it happens through the next and prev page pointers on the leaf page
	int is_stacked;

	union
	{
		struct
		{
			int lock_type; // For stacked iterator lock_type = READ_LOCK, WRITE_LOCK & LEAF_ONLY_WRITER_LOCK
			locked_pages_stack lps;	// use this if is_stacked is set
		};
		persistent_page curr_page; // else use this
	};

	// curr_tuple_index is the index of the tuple, that the iterator points
	// in the curr_page (a bplus_tree_leaf page)
	uint32_t curr_tuple_index;

	const bplus_tree_tuple_defs* bpttd_p;

	const page_access_methods* pam_p;
	/*
		pmm_p must be provided if lock_type is anything other than READ_LOCK
	*/

	// WRITE_LOCK or READ_LOCK, for the leaves, is identified by pmm_p == NULL or not
	// i.e. if pmm_p == NULL ? READ_LOCK : WRITE_LOCK for leaf pages
	// all interior pages in case of a stacked_iterator are only READ_LOCK-ed
	const page_modification_methods* pmm_p;
	// for a read-only bplus_tree_iterator, pmm_p = NULL
};

#define LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE UINT32_MAX

// curr_tuple_index if LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE then the iterator will point to the last
// after the successfull call to this function (return value != NULL), the lps/curr_page is solely owned by the bplus_tree_iterator
// and lps/curr_page gets deinitialized/freed by the bplus_tree_iterator only after delete_bplus_tree_iterator() call
bplus_tree_iterator* get_new_bplus_tree_iterator(persistent_page curr_page, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p);
bplus_tree_iterator* get_new_bplus_tree_stacked_iterator(locked_pages_stack lps, uint32_t curr_tuple_index, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p);


// bpi_p is assumed to be garbage initialized, and is fully initialized only on success
// provide either a valid non empty lps or a root_page_id
// unpin success all lps pages are transferred to the bpi_p and lps is destroyed.
// on failure lps is left untouched
// on abort_error all locks held in the lps are released
int initialize_bplus_tree_iterator_stacked(bplus_tree_iterator* bpi_p, uint64_t root_page_id, locked_pages_stack lps, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
int initialize_bplus_tree_iterator_unstacked(bplus_tree_iterator* bpi_p, uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#include<bplus_tree_iterator_public.h>

#endif