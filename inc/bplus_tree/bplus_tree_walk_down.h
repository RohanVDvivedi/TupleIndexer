#ifndef BPLUS_TREE_WALK_DOWN_H
#define BPLUS_TREE_WALK_DOWN_H

/*
**	This file provides different utility functions to walk down a bplus tree and initialize the locked_pages_stack for a walk down
*/

#include<locked_pages_stack.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>

#include<persistent_page_functions.h>

/*
** 	the initialize_locked_pages_stack_for_walk_down function and the walk_down_for_stacked_iterator, walk_next_for_stacked_iterator and walk_next_for_stacked_iterator
**	does support the lock_type READ_LOCK_INTERIOR_WRITE_LOCK_LEAF
*/

// the capacity of the stack will be same as the height of the tree i.e. the number of nodes from root to leaf
// only lock to the root page will be acquired with the given lock type
// on an abort error, it will return an empty locked pages stack, with no pages kept locked
locked_pages_stack initialize_locked_pages_stack_for_walk_down(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// below are walk down functions
// you must lock atleast the root page, in the locked_pages_stack_p, before calling these functions
// no page locks are kept acquired on an abort
// as soon as an abort is encountered all page locks (including the lock on the root page) are dumpled,
// leaving no pages on the locked_pages_stack

int walk_down_locking_parent_pages_for_split_insert_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int walk_down_locking_parent_pages_for_merge_using_key(locked_pages_stack* locked_pages_stack_p, const void* key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int walk_down_locking_parent_pages_for_merge_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int walk_down_locking_parent_pages_for_update_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t* release_for_split, uint32_t* release_for_merge, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// find type is essential to guide the walk_down for the stacked_iterator
typedef enum find_type find_type;
enum find_type
{
	MIN_TUPLE,
	LESSER_THAN_KEY,
	LESSER_THAN_EQUALS_KEY,
	GREATER_THAN_EQUALS_KEY,
	GREATER_THAN_KEY,
	MAX_TUPLE,
};

int walk_down_locking_parent_pages_for_stacked_iterator_using_key(locked_pages_stack* locked_pages_stack_p, const void* key, uint32_t key_element_count_concerned, find_type f_type, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// go next for the current state of the stack
int walk_down_next_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// go prev for the current state of the stack
int walk_down_prev_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// below function is to be used when you are done with the locked pages stack
// it is recallable, it does not fail on abort
void release_all_locks_and_deinitialize_stack_reenterable(locked_pages_stack* locked_pages_stack_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif