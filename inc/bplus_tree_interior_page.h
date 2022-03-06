#ifndef BPLUS_TREE_INTERIOR_PAGE_H
#define BPLUS_TREE_INTERIOR_PAGE_H

#include<stdint.h>

#include<common_page_header.h>
#include<bplus_tree_page.h>

typedef struct bplus_tree_interior_page_header bplus_tree_interior_page_header;
struct bplus_tree_interior_page_header
{
	uint32_t least_keys_page;	// link to child page having keys lesser than the least key on this page
};

#define sizeof_INTERIOR_PAGE_HEADER (sizeof(page_header) + sizeof(bplus_tree_page_header) + sizeof(bplus_tree_interior_page_header))

uint32_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, uint32_t page_size);

#endif