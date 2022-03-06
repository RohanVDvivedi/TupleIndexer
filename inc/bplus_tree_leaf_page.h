#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

#include<common_page_header.h>
#include<bplus_tree_page.h>

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	uint32_t prev_page;		// towards all lesser data
	uint32_t next_page;	// towards all greater data
};

#define sizeof_LEAF_PAGE_HEADER (sizeof(page_header) + sizeof(bplustree_page_header) + sizeof(bplus_tree_leaf_page_header))

uint32_t get_next_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);

uint32_t get_prev_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);

#endif