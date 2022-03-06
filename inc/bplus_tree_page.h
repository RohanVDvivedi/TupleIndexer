#ifndef BPLUS_TREE_PAGE_H
#define BPLUS_TREE_PAGE_H

#include<stdint.h>

typedef struct bplus_tree_page_header bplus_tree_page_header;
struct bplus_tree_page_header
{
	// 0 for the leaf page
	uint32_t level;
};

uint32_t get_level_of_bplus_tree_page(const void* page, uint32_t page_size);

#endif