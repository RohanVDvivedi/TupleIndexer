#ifndef BPLUS_TREE_INTERIOR_PAGE_HEADER_H
#define BPLUS_TREE_INTERIOR_PAGE_HEADER_H

#include<common_page_header.h>
#include<bplus_tree_page.h>
#include<bplus_tree_tuple_definitions.h>

uint32_t sizeof_INTERIOR_PAGE_HEADER(const bplus_tree_tuple_defs* bpttd_p);

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);
void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p);

int is_last_page_of_level_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);
void set_is_last_page_of_level_of_bplus_tree_interior_page(void* page, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p);

// prints header of bplus_tree interior page
void print_bplus_tree_interior_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

#endif