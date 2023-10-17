#ifndef BPLUS_TREE_FIND_UTIL_H
#define BPLUS_TREE_FIND_UTIL_H

#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_data_access_methods.h>

// on success locked_leaf will be returned with lock_type lock on it
int read_couple_locks_until_leaf_using_key(uint64_t root_page_id, const void* key, int lock_type, persistent_page* locked_leaf, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// on success locked_leaf will be returned with lock_type lock on it
int read_couple_locks_until_leaf_using_record(uint64_t root_page_id, const void* record, int lock_type, persistent_page* locked_leaf, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#endif