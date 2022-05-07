#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<stdint.h>

#include<bplus_tree_interior_page_header.h>

/*
*	child index start from (-1) and end at (tuple_count - 1)
*	child page_id at index = (-1) => is same as the "least_keys_page_id" as given in the bplus_tree_interior_page_header
*	all other child page_id are stored as the last attributes of the corresponding tuples
*/

// this the index of the tuple in the interior page that you should follow
// you may cache this, it may help in case of a split
uint32_t find_child_index_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// returns the page_id stored with the corresponding tuple at index, in its attribute "child_page_id" 
uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p);

#endif