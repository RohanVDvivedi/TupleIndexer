#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	uint32_t prev_page;		// towards all lesser data
	uint32_t next_page;	// towards all greater data
};

#define sizeof_LEAF_PAGE_HEADER

uint32_t get_next_of_bplus_tree_leaf_page(void* page);

uint32_t get_prev_of_bplus_tree_leaf_page(void* page);

#endif