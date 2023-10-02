#ifndef COMMON_PAGE_HEADER_H
#define COMMON_PAGE_HEADER_H

#include<bplus_tree_tuple_definitions.h>
#include<page_modification_methods.h>

#include<stdint.h>

typedef enum page_type page_type;
enum page_type
{
	BPLUS_TREE_LEAF_PAGE,
	BPLUS_TREE_INTERIOR_PAGE,
	// add more page_types as you wish at most ((2^16) - 1) page_types can be supported
};

typedef struct common_page_header common_page_header;
struct common_page_header
{
	page_type type;
};

/*
**		system header is prefix to even the common page header
**		size of this system header is equal to the what was specified in bplus_tree_tuple_definitions object
*/

uint32_t get_offset_of_page_type_header(const bplus_tree_tuple_defs* bpttd_p);

uint32_t get_size_of_page_type_header();

page_type get_type_of_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);

common_page_header get_common_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

void serialize_common_page_header(void* hdr_serial, const common_page_header* cph_p);

void set_common_page_header(persistent_page ppage, const common_page_header* cph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p);

void print_common_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

#endif