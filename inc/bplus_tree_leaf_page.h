#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

#include<common_page_header.h>
#include<bplus_tree_page.h>

typedef struct bplus_tree_leaf_page_header1 bplus_tree_leaf_page_header1;
struct bplus_tree_leaf_page_header1
{
	uint8_t prev_page_id;	// towards all lesser data
	uint8_t next_page_id;	// towards all greater data
};

typedef struct bplus_tree_leaf_page_header2 bplus_tree_leaf_page_header2;
struct bplus_tree_leaf_page_header2
{
	uint16_t prev_page_id;	// towards all lesser data
	uint16_t next_page_id;	// towards all greater data
};

typedef struct bplus_tree_leaf_page_header4 bplus_tree_leaf_page_header4;
struct bplus_tree_leaf_page_header4
{
	uint32_t prev_page_id;	// towards all lesser data
	uint32_t next_page_id;	// towards all greater data
};

typedef struct bplus_tree_leaf_page_header8 bplus_tree_leaf_page_header8;
struct bplus_tree_leaf_page_header8
{
	uint64_t prev_page_id;	// towards all lesser data
	uint64_t next_page_id;	// towards all greater data
};

uint32_t sizeof_LEAF_PAGE_HEADER();

// getter and setter for next_page_id of the page
uint64_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);
void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint64_t page_id);

// getter and setter for prev_page_id of the page
uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size);
void set_prev_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint64_t page_id);

#endif