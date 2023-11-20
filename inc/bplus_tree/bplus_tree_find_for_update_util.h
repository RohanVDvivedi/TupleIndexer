#ifndef BPLUS_TREE_FIND_FOR_UPDATE_UTIL_H
#define BPLUS_TREE_FIND_FOR_UPDATE_UTIL_H

#include<locked_pages_stack.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_access_methods.h>

// you must lock the root page, in the locked_pages_stack_p, before calling this function
// all page locks (even the root_page_lock) will be released upon an abort_error
// this function ensures that record may not the actual record that would be inserted or deleted, it only finds the parent to leaf links that this record may go to
// you may insert, delete or update, your wish -> but minimal locks will be held to allow for split or merge
// release_for_split and release_for_merge will be set to the page lock counts that you may release from the bottom to perform the corresponding operation
int walk_down_locking_parent_pages_for_update_using_record(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t* release_for_split, uint32_t* release_for_merge, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif