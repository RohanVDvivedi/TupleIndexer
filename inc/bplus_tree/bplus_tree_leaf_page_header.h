#ifndef BPLUS_TREE_LEAF_PAGE_HEADER_H
#define BPLUS_TREE_LEAF_PAGE_HEADER_H

#include<common_page_header.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	common_page_header parent;

	// all leaf pages are doubly linked, with pointers (page_id-s) to next and prev pages
	uint64_t next_page_id;

	uint32_t prev_page_id;
};

#define sizeof_BPLUS_TREE_LEAF_PAGE_HEADER get_offset_to_end_of_bplus_tree_leaf_page_header

uint32_t get_offset_to_end_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p);

uint64_t get_next_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

bplus_tree_leaf_page_header get_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

void serialize_bplus_tree_leaf_page_header(void* hdr_serial, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p);

void set_bplus_tree_leaf_page_header(persistent_page* ppage, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of bplus_tree leaf page
void print_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

#endif