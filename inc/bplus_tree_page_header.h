#ifndef BPLUS_TREE_PAGE_H
#define BPLUS_TREE_PAGE_H

#include<bplus_tree_tuple_definitions.h>
#include<page_modification_methods.h>

#include<common_page_header.h>

#include<stdint.h>

typedef struct bplus_tree_page_header bplus_tree_page_header;
struct bplus_tree_page_header
{
	common_page_header parent;

	// level of the page in bplus_tree,
	// = 0 -> leaf page
	// > 0 -> interior page
	uint32_t level;
};

uint32_t get_offset_of_bplus_tree_page_header(const bplus_tree_tuple_defs* bpttd_p);

uint32_t get_size_of_bplus_tree_page_header();

uint32_t get_level_of_bplus_tree_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);

int is_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);

bplus_tree_page_header get_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

void serialize_bplus_tree_page_header(void* hdr_serial, const bplus_tree_page_header* bptph_p, const bplus_tree_tuple_defs* bpttd_p);

void set_bplus_tree_page_header(persistent_page ppage, const bplus_tree_page_header* bptph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p);

void print_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

#endif