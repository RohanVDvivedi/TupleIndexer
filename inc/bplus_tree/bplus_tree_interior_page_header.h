#ifndef BPLUS_TREE_INTERIOR_PAGE_HEADER_H
#define BPLUS_TREE_INTERIOR_PAGE_HEADER_H

#include<common_page_header.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>

typedef struct bplus_tree_interior_page_header bplus_tree_interior_page_header;
struct bplus_tree_interior_page_header
{
	common_page_header parent;

	// level of the page in bplus_tree,
	// always > 0 interior page
	// 1 -> interior page level right above the leaf page
	uint32_t level;

	// all leaf pages are doubly linked, with pointers (page_id-s) to next and prev pages
	uint64_t least_keys_page_id;

	// flag that suggests if this interior page is last one on this bplus_tree level
	int is_last_page_of_level;
};

#define sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER get_offset_to_end_of_bplus_tree_interior_page_header

uint32_t get_offset_to_end_of_bplus_tree_interior_page_header(const bplus_tree_tuple_defs* bpttd_p);

uint32_t get_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

int is_last_page_of_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

bplus_tree_interior_page_header get_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

void serialize_bplus_tree_interior_page_header(void* hdr_serial, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p);

void set_bplus_tree_interior_page_header(persistent_page* ppage, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of bplus_tree interior page
void print_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

#endif