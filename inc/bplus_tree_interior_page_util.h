#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<bplus_tree_util.h>

int is_interior_node_page(const void* page);

// returns page_id, to search for find_like tuple
uint32_t find_in_interior_node_page(const void* page, const void* like_key);

// inserts a new_index_tuple to the intr_page
// if the insert fails due to the page being out of space
// then this interior page is split and the non-NULL index tuple is created
// now this non Null index tuple needs to be inserted into the parent interior page
// the retunred tuple is allocated using malloc and the calling function has to deallocate it after use/insert
const void* insert_or_split_interior_node_page(const void* intr_page, const void* new_index_tuple);

#endif