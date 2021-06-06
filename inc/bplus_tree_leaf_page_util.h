#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<bplus_tree.h>

// to initialize a leaf page
int init_leaf_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

// returns index to the first record tuple in the page, that has key that equals the like_key
uint16_t find_in_leaf_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds);

// below functions manage splits and merges of the leaf_pages of the b+tree

// returns the tuple (of bpttds->tuple_def) that we need to insert into the parent page
// returns non NULL tuple if the page was split
void* split_insert_leaf_page(void* page_to_be_split, const void* new_record, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

// returns true (1), if parent_index_record needs to be deleted from the parent_page
int merge_leaf_pages(void* page, const void* parent_index_record, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

#endif