#ifndef BPLUS_TREE_MERGE_UTIL_H
#define BPLUS_TREE_MERGE_UTIL_H

#include<locked_pages_stack.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_data_access_methods.h>
#include<opaque_page_modification_methods.h>

// you must lock the root page, in the locked_pages_stack_p, before calling this function
int walk_down_locking_parent_pages_for_merge_using_key(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// you must lock the root page, in the locked_pages_stack_p, before calling this function
int walk_down_locking_parent_pages_for_merge_using_record(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#endif