#ifndef BPLUS_TREE_PAGE_HEADER_H
#define BPLUS_TREE_PAGE_HEADER_H

#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>

#include<common_page_header.h>
#include<bplus_tree_interior_page_header.h>

/*
**	utility functions for bplus_tree pages
*/

static inline int is_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// =0 => leaf page
// >0 => interior page
static inline uint32_t get_level_of_bplus_tree_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// inline implementations

static inline int is_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_common_page_header(ppage, bpttd_p->pas_p).type == BPLUS_TREE_LEAF_PAGE;
}

static inline uint32_t get_level_of_bplus_tree_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	// leaf page is at level 0
	if(is_bplus_tree_leaf_page(ppage, bpttd_p))
		return 0;

	// else it is an interior page starting at 1
	return get_level_of_bplus_tree_interior_page(ppage, bpttd_p);
}

#endif