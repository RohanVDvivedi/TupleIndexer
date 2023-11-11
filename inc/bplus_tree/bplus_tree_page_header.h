#ifndef BPLUS_TREE_PAGE_HEADER_H
#define BPLUS_TREE_PAGE_HEADER_H

#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>

/*
**	utility functions for bplus_tree pages
*/

int is_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// =0 => leaf page
// >0 => interior page
uint32_t get_level_of_bplus_tree_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

#endif