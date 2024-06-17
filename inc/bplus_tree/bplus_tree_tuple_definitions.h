#ifndef BPLUS_TREE_TUPLE_DEFINITIONS_H
#define BPLUS_TREE_TUPLE_DEFINITIONS_H

#include<bplus_tree_tuple_definitions_public.h>

// builds an index_entry from record_tuple and a child_page_id
int build_index_entry_from_record_tuple_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry);

// builds an index_entry from key and a child_page_id
int build_index_entry_from_key_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry);

#endif