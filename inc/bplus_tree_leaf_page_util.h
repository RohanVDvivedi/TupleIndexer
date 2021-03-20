#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<bplus_tree_util.h>

int is_leaf_node_page(const void* page);

uint32_t get_next_leaf_node_page(const void* page);
uint32_t get_prev_leaf_node_node(const void* page);

#endif