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

#include<bplus_tree_walk_down_custom_lock_type.h>

// the capacity of the stack will be same as the height of the tree i.e. the number of nodes from root to leaf
// only lock to the root page will be acquired with the given lock type
// on an abort error, it will return an empty locked pages stack, with no pages kept locked
locked_pages_stack initialize_locked_pages_stack_for_walk_down(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// below are walk down functions
// you must lock atleast the root page, in the locked_pages_stack_p, before calling these functions
// no page locks are kept acquired on an abort
// as soon as an abort is encountered all page locks (including the lock on the root page) are dumpled,
// leaving no pages on the locked_pages_stack

int walk_down_locking_parent_pages_for_split_insert(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record, int is_key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define walk_down_locking_parent_pages_for_split_insert_using_key(locked_pages_stack_p, key, bpttd_p, pam_p, transaction_id, abort_error)       walk_down_locking_parent_pages_for_split_insert(locked_pages_stack_p, key, 1, bpttd_p, pam_p, transaction_id, abort_error)
#define walk_down_locking_parent_pages_for_split_insert_using_record(locked_pages_stack_p, record, bpttd_p, pam_p, transaction_id, abort_error) walk_down_locking_parent_pages_for_split_insert(locked_pages_stack_p, record, 0, bpttd_p, pam_p, transaction_id, abort_error)

// get count of parent pages for the locked pages stack, that can be unlocked if we are sure of performing a split_insert in the locked leaf
// it is agnostic of the tuple to be inserted, it does not check leaf page
uint32_t count_unlockable_parent_pages_for_split_insert(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p);

int walk_down_locking_parent_pages_for_merge(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record, int is_key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define walk_down_locking_parent_pages_for_merge_using_key(locked_pages_stack_p, key, bpttd_p, pam_p, transaction_id, abort_error)       walk_down_locking_parent_pages_for_merge(locked_pages_stack_p, key, 1, bpttd_p, pam_p, transaction_id, abort_error)
#define walk_down_locking_parent_pages_for_merge_using_record(locked_pages_stack_p, record, bpttd_p, pam_p, transaction_id, abort_error) walk_down_locking_parent_pages_for_merge(locked_pages_stack_p, record, 0, bpttd_p, pam_p, transaction_id, abort_error)

// get count of parent pages for the locked pages stack, that can be unlocked if we are sure of performing a merge in the locked leaf
// it is agnostic of the tuple to be deleted, it does not check leaf page
uint32_t count_unlockable_parent_pages_for_merge(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p);

int walk_down_locking_parent_pages_for_update(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record, int is_key, uint32_t* release_for_split, uint32_t* release_for_merge, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define walk_down_locking_parent_pages_for_update_using_key(locked_pages_stack_p, key, release_for_split, release_for_merge, bpttd_p, pam_p, transaction_id, abort_error)       walk_down_locking_parent_pages_for_update(locked_pages_stack_p, key, 1, release_for_split, release_for_merge, bpttd_p, pam_p, transaction_id, abort_error)
#define walk_down_locking_parent_pages_for_update_using_record(locked_pages_stack_p, record, release_for_split, release_for_merge, bpttd_p, pam_p, transaction_id, abort_error) walk_down_locking_parent_pages_for_update(locked_pages_stack_p, record, 0, release_for_split, release_for_merge, bpttd_p, pam_p, transaction_id, abort_error)

#include<find_position.h>

// in any lock_type, the interior pages are always locked with read locks and the locks on interior pages are not held any more than it could be required for latch crabbing
// you are returned a leaf page with the provided lock_type
persistent_page walk_down_for_iterator(uint64_t root_page_id, const void* key_OR_record, int is_key, uint32_t key_element_count_concerned, find_position f_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define walk_down_for_iterator_using_key(root_page_id, key, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)       walk_down_for_iterator(root_page_id, key, 1, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)
#define walk_down_for_iterator_using_record(root_page_id, record, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error) walk_down_for_iterator(root_page_id, record, 0, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)

int walk_down_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record, int is_key, uint32_t key_element_count_concerned, find_position f_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define walk_down_locking_parent_pages_for_stacked_iterator_using_key(locked_pages_stack_p, key, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)       walk_down_locking_parent_pages_for_stacked_iterator(locked_pages_stack_p, key, 1, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)
#define walk_down_locking_parent_pages_for_stacked_iterator_using_record(locked_pages_stack_p, record, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error) walk_down_locking_parent_pages_for_stacked_iterator(locked_pages_stack_p, record, 0, key_element_count_concerned, f_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error)

// go next for the current state of the stack, to the next page of the current top leaf page
// returns 0, if it resulst in end of scan
int walk_down_next_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// go prev for the current state of the stack, to the prev page of the current top leaf page
// returns 0, if it resulst in end of scan
int walk_down_prev_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// the above two functions when called on the last or first page of the scan respectively, will release all the locks and return failure with an empty locked pages stack
// to avoid this you can use the below function to check if these calls will succeed

int can_walk_down_next_locking_parent_pages_for_stacked_iterator(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p);

int can_walk_down_prev_locking_parent_pages_for_stacked_iterator(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p);

// relases parent interior page locks when the child_index for the key_OR_record1, key_or_record2 and the current child_index are the same
// f_pos1 can only be GREATER_THAN, GREATER_THAN_EQUALS OR MIN and f_pos2 can only be LESSER_THAN, LESSER_THAN_EQUALS or MAX
// a must condition -> key_OR_record1 <= key_OR_record2
// MIN and MAX here implies to -infinity and _infinity keys only, and no the actual MIN and MAX tuples in the bplus_tree
int narrow_down_range_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record1, find_position f_pos1, const void* key_OR_record2, find_position f_pos2, int is_key, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
#define narrow_down_range_for_stacked_iterator_using_keys(locked_pages_stack_p, key1, f_pos1, key2, f_pos2, key_element_count_concerned, bpttd_p, pam_p, transaction_id, abort_error)          narrow_down_range_for_stacked_iterator(locked_pages_stack_p, key1, f_pos1, key2, f_pos2, 1, key_element_count_concerned, bpttd_p, pam_p, transaction_id, abort_error)
#define narrow_down_range_for_stacked_iterator_using_records(locked_pages_stack_p, record1, f_pos1, record2, f_pos2, key_element_count_concerned, bpttd_p, pam_p, transaction_id, abort_error) narrow_down_range_for_stacked_iterator(locked_pages_stack_p, record1, f_pos1, record2, f_pos2, 0, key_element_count_concerned, bpttd_p, pam_p, transaction_id, abort_error)

// this below function returns 1 only if your stacked_iterator's position (the locked pages stack) is correct for insertion of the key_OR_record
// this is a test that must be passed for any locked pages stack for insertion
// this function only returns conclusive results if the locked_pages_stack holds locks all the way from root to leaf, else the result of this function is not a definite surety
int check_is_at_rightful_position_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, const void* key_OR_record, int is_key, bplus_tree_tuple_defs* bpttd_p);
#define check_is_at_rightful_position_for_stacked_iterator_using_key(locked_pages_stack_p, key, bpttd_p) 		check_is_at_rightful_position_for_stacked_iterator(locked_pages_stack_p, key, 1, bpttd_p)
#define check_is_at_rightful_position_for_stacked_iterator_using_record(locked_pages_stack_p, record, bpttd_p) 	check_is_at_rightful_position_for_stacked_iterator(locked_pages_stack_p, record, 0, bpttd_p)

// below function is to be used when you are done with the locked pages stack
// it is recallable, it does not fail on abort
void release_all_locks_and_deinitialize_stack_reenterable(locked_pages_stack* locked_pages_stack_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif