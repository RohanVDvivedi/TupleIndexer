#ifndef GET_PAGE_TYPE_H
#define GET_PAGE_TYPE_H

typedef enum page_type page_type;
enum page_type
{
	BPLUS_TREE_LEAF_PAGE = 0, // values will 0 and so on
	BPLUS_TREE_INTERIOR_PAGE,
	ARRAY_TABLE_PAGE,
	LINKED_PAGE_LIST_PAGE,
	WORM_HEAD_PAGE,
	WORM_ANY_PAGE, // for any page other than the head of the worm
	HEAP_PAGE,
	BITMAP_PAGE,
	// add more page_types as you wish at most ((2^16) - 1) page_types can be supported
};

extern char const * const page_type_string[];

#endif