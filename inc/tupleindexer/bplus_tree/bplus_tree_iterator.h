#ifndef BPLUS_TREE_ITERATOR_H
#define BPLUS_TREE_ITERATOR_H

#include<stdint.h>

#include<locked_pages_stack.h>
#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>
#include<find_position.h>

// this iterator can only be used to reading leaf tuples of the b+tree

typedef struct bplus_tree_iterator bplus_tree_iterator;
struct bplus_tree_iterator
{
	uint64_t root_page_id;

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

bplus_tree_iterator* get_new_bplus_tree_stacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
bplus_tree_iterator* get_new_bplus_tree_unstacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// bpi_p is assumed to be garbage initialized, and is fully initialized only on success
// lps must not be NULL
// provide either a valid non empty lps or a root_page_id
// upon success all lps pages are transferred to the bpi_p
// on abort_error all locks held in the lps are released
// lps has to be deinitialized after any call to this function
int initialize_bplus_tree_stacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, locked_pages_stack* lps, const void* key_OR_record, int is_key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
#define initialize_bplus_tree_stacked_iterator_using_key(bpi_p, root_page_id, lps, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)			initialize_bplus_tree_stacked_iterator(bpi_p, root_page_id, lps, key, 1, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)
#define initialize_bplus_tree_stacked_iterator_using_record(bpi_p, root_page_id, lps, record, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)		initialize_bplus_tree_stacked_iterator(bpi_p, root_page_id, lps, record, 0, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)
int initialize_bplus_tree_unstacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// private function to get the current lockd leaf page
persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p);

#include<bplus_tree_iterator_public.h>

#endif