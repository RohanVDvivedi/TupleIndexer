#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<stdint.h>

#include<bplus_tree_interior_page_header.h>

// when you want tuples in descending order from the key
uint32_t find_greater_equals_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// when you want tuples in ascending order from the key
uint32_t find_lesser_equals_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

#endif