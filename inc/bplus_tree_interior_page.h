#ifndef BPLUS_TREE_INTERIOR_PAGE_H
#define BPLUS_TREE_INTERIOR_PAGE_H

#include<common_page_header.h>
#include<bplus_tree_page.h>
#include<bplus_tree_tuple_definitions.h>

uint32_t sizeof_INTERIOR_PAGE_HEADER(bplus_tree_tuple_defs* bpttd_p);

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, bplus_tree_tuple_defs* bpttd_p);
void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, bplus_tree_tuple_defs* bpttd_p);

#endif