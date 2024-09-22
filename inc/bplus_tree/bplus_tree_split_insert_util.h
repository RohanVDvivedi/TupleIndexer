#ifndef BPLUS_TREE_SPLIT_INSERT_UTIL_H
#define BPLUS_TREE_SPLIT_INSERT_UTIL_H

#include<locked_pages_stack.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// the locked_pages_stack may contain only the pages in the chain that will participate in the split insert
// i.e. the root_page may not be in the locked pages stack
// all the locks are released by this function, only on an abort_error
// this function always returns 1, except on an abort_error
// in absence of an abort error, locks to all pages untouched are left as is,
// so do call release_all_locks_and_deinitialize_stack_reenterable once you are done with the stack
// insertion_index is an optional parameter, if you do not know it set it to INVALID_TUPLE_INDEX, this function inherently does not and will not check for duplicate insertions
int split_insert_and_unlock_pages_up(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t insertion_index, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif