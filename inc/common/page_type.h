#ifndef GET_PAGE_TYPE_H
#define GET_PAGE_TYPE_H

typedef enum page_type page_type;
enum page_type
{
	BPLUS_TREE_LEAF_PAGE,
	BPLUS_TREE_INTERIOR_PAGE,
	PAGE_TABLE_PAGE,
	// add more page_types as you wish at most ((2^16) - 1) page_types can be supported
};

page_type get_type_of_page(const persistent_page* ppage, const page_access_specs* pas_p);

#endif