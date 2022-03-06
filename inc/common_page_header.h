#ifndef COMMON_PAGE_HEADER_H
#define COMMON_PAGE_HEADER_H

#include<stdint.h>

typedef enum page_type page_type;
enum page_type
{
	BPLUS_TREE_INTERIOR_PAGE,
	BPLUS_TREE_LEAF_PAGE,
};

typedef struct page_header page_header;
struct page_header
{
	// page type of the page
	uint16_t type;
};

page_type get_type_of_page(const void* page, uint32_t page_size);

#endif