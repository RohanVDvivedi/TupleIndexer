#ifndef BPLUS_TREE_INDEX_TUPLE_FUNCTIONS_UTIL_H
#define BPLUS_TREE_INDEX_TUPLE_FUNCTIONS_UTIL_H

#include<bplus_tree_tuple_definitions.h>

// get child_page_id from the index_tuple
uint64_t get_child_page_id_from_index_tuple(const void* index_tuple, bplus_tree_tuple_defs* bpttd_p);

// update/insert child_page_id in the index_tuple
void set_child_page_id_in_index_tuple(void* index_tuple, uint64_t child_page_id, bplus_tree_tuple_defs* bpttd_p);

#endif