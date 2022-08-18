#ifndef BPLUS_TREE_PAGE_H
#define BPLUS_TREE_PAGE_H

#include<stdint.h>

uint32_t get_offset_of_bplus_tree_page_level_header(const void* page, uint32_t page_size);

uint32_t get_size_of_bplus_tree_page_level_header();

// getter and setter for the level of the page
uint32_t get_level_of_bplus_tree_page(const void* page, uint32_t page_size);
void set_level_of_bplus_tree_page(void* page, uint32_t page_size, uint32_t level);

void print_bplus_tree_page_header(const void* page, uint32_t page_size);

int is_bplus_tree_leaf_page(const void* page, uint32_t page_size);

#endif