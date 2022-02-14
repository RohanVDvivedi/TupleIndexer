#ifndef COMMON_PAGE_HEADER_H
#define COMMON_PAGE_HEADER_H

typedef enum page_type page_type;
{
	BPLUS_TREE_ROOT_PAGE,
	BPLUS_TREE_INTERIOR_PAGE,
	BPLUS_TREE_LEAF_PAGE,
};

typedef struct page_header page_header;
struct page_header
{
	uint16_t page_type;
};

#endif