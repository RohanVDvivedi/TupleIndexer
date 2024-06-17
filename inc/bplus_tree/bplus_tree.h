#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include<bplus_tree_iterator_public.h>
#include<bplus_tree_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>
#include<find_position.h>

// returns pointer to the root page of the newly created bplus_tree
uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// this macro can be passed to key_element_count_concerned (to find_in_bplus_tree), to consider all the key_elements as found in bpttd_p(->key_element_count)
#define KEY_ELEMENT_COUNT UINT32_C(-1)

// returns a bplus_tree_iterator to read from key 
// here the key_element_count_concerned suggests the number of elements of the key that would be considered for find operation
// the key == NULL and find_pos == GREATER_THAN, then the iterator will point to the first tuple of the bplus_tree
// the key == NULL and find_pos == LESSER_THAN, then the iterator will point to the last tuple of the bplus_tree
// it may return NULL, only on an abort_error
// if pmm_p == NULL, then iterator would be read-only
// if is_stacked == 1, the iterator uses parent linkages for the next leaf and prev leaf page iteration
bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int is_stacked, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

typedef struct update_inspector update_inspector;
struct update_inspector
{
	const void* context;

	// the operation for an update will be performed only if the update_inspector returns 1 and the cancel_update_callback has not been called, 0 implies no operation
	// cancel_update_callback simply, releases all write locks and downgrades lock on the leaf page to read lock, you may still read the old_record after calling cancel_update_callback
	// cancel_update_callback must not be called more than once
	// you must returns with 0, immidiately with 0, if te cancel_update_callback returns with an abort_error
	// new_record can be modified by this function, (but mind well to keep all the key elements of the record un touched)
	// upon the return of 1 from this function
	// if(old_record == NULL && *new_record == NULL) -> do nothing
	// else if(old_record != NULL && *new_record == NULL) -> do delete of the old_record
	// else if(old_record == NULL && *new_record != NULL) -> do insert for the new_record
	// else if(old_record != NULL && *new_record != NULL) -> do update old_record with the new_record
	int (*update_inspect)(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error);
};

// to find and read a record, then inspect it with the ui_p, and then proceed to update it
// update may fail for an abort_error OR if the update_inspector returns so that no update is required
int inspected_update_in_bplus_tree(uint64_t root_page_id, void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// insert record in bplus_tree
// insert may fail on an abort_error OR if a record with the same key already exists in the bplus_tree
int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// delete a record given by key
// delete may fail on an abort_error OR if a record with the given key, does not exist in the bplus_tree
int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the bplus_tree
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given bplus_tree
int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the bplus_tree
// it may return an abort_error, unable to print all of the bplus_tree pages
void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif