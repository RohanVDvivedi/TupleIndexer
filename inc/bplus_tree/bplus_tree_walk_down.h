#ifndef BPLUS_TREE_WALK_DOWN_H
#define BPLUS_TREE_WALK_DOWN_H

/*
**	This file provides different utility functions to walk down a bplus tree and initialize the locked_pages_stack for a walk down
*/

#include<locked_pages_stack.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>

#include<persistent_page_functions.h>

// the capacity of the stack will be same as the height of the tree i.e. the number of nodes from root to leaf
// only lock to the root page will be acquired with the given lock type
// on an abort error, it will return an empty locked pages stack, with no pages kept locked
locked_pages_stack initialize_locked_pages_stack_for_walk_down(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif