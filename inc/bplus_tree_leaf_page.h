#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

#include<common_page_header.h>
#include<bplus_tree_page.h>

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	uint32_t prev_page_id;	// towards all lesser data
	uint32_t next_page_id;	// towards all greater data
};

#define sizeof_LEAF_PAGE_HEADER (sizeof(page_header) + sizeof(bplustree_page_header) + sizeof(bplus_tree_leaf_page_header))

// getter and setter for next_page_id of the page
uint32_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);
void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint32_t page_id);

// getter and setter for prev_page_id of the page
uint32_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);
void set_prev_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint32_t page_id);

#endif