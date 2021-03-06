#ifndef COMMON_PAGE_HEADER_H
#define COMMON_PAGE_HEADER_H

#include<stdint.h>

typedef enum page_type page_type;
enum page_type
{
	BPLUS_TREE_LEAF_PAGE,
	BPLUS_TREE_INTERIOR_PAGE,
};

uint32_t get_size_of_page_type_header();

// getter and setter for page type of the page
page_type get_type_of_page(const void* page, uint32_t page_size);
void set_type_of_page(void* page, uint32_t page_size, page_type type);

void print_common_page_header(const void* page, uint32_t page_size);

#endif