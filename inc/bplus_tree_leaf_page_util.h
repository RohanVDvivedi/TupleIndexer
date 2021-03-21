#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<bplus_tree_util.h>

int is_leaf_node_page(const void* page);

uint32_t get_next_leaf_node_page(const void* page);
uint32_t get_prev_leaf_node_node(const void* page);

// returns index to the record tuple in the page, that equals the find_like tuple
uint16_t find_in_interior_node_page(const void* page, const void* like_key);

// inserts a new_record_tuple to the leaf_page
// if the insert fails due to the page being out of space
// then this leaf page is split and the non-NULL index tuple is created
// now this non Null index tuple needs to be inserted into the parent interior page
// the retunred tuple is allocated using malloc and the calling function has to deallocate it after use/insert
const void* insert_or_split_leaf_node_page(const void* intr_page, const void* new_record_tuple);

#endif