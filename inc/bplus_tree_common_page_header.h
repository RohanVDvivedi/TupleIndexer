#ifndef BPLUS_TREE_PAGE_H
#define BPLUS_TREE_PAGE_H

#include<bplus_tree_tuple_definitions.h>

#include<stdint.h>

uint32_t get_offset_of_bplus_tree_page_level_header(const bplus_tree_tuple_defs* bpttd_p);

uint32_t get_size_of_bplus_tree_page_level_header();

// getter and setter for the level of the page
uint32_t get_level_of_bplus_tree_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);
void set_level_of_bplus_tree_page(void* page, uint32_t level, const bplus_tree_tuple_defs* bpttd_p);

void print_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

int is_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p);

#endif