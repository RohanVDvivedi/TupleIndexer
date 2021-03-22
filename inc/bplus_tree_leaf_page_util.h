#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<bplus_tree_util.h>

int is_leaf_page(const void* page);

uint32_t get_next_leaf_page(const void* page);
uint32_t get_prev_leaf_page(const void* page);

// returns number of records in the leaf page
uint16_t get_record_count_in_leaf_page(const void* page);

// returns a pointer in the page that points to index-th record, and has tuple definition of that of a record_def
const void* get_record_from_leaf_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds);

// returns index to the first record tuple in the page, that has key that equals the like_key
uint16_t find_in_leaf_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds);

// inserts a new_record_tuple to the leaf_page
// if the insert fails due to the page being out of space
// then this leaf page is split and the non-NULL index tuple is created
// now this non Null index tuple needs to be inserted into the parent interior page
// the retunred tuple is allocated using malloc and the calling function has to deallocate it after use/insert
const void* insert_or_split_leaf_page(const void* intr_page, const void* new_record_tuple, const bplus_tree_tuple_defs* bpttds);

#endif