#ifndef BPLUS_TREE_LEAF_PAGE_HEADER_H
#define BPLUS_TREE_LEAF_PAGE_HEADER_H

#include<common_page_header.h>
#include<bplus_tree_common_page_header.h>
#include<bplus_tree_tuple_definitions.h>

uint32_t sizeof_LEAF_PAGE_HEADER(const bplus_tree_tuple_defs* bpttd_p);

// getter and setter for next_page_id of the page
uint64_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);
void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p);

// getter and setter for prev_page_id of the page
uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);
void set_prev_page_id_of_bplus_tree_leaf_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p);

// prints header of bplus_tree leaf page
void print_bplus_tree_leaf_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

#endif