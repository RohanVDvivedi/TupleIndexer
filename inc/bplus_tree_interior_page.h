#ifndef BPLUS_TREE_INTERIOR_PAGE_H
#define BPLUS_TREE_INTERIOR_PAGE_H

typedef struct bplus_tree_interior_page_header bplus_tree_interior_page_header;
struct bplus_tree_interior_page_header
{
	uint32_t least_link;	// link to child page having data lesser than the least key on this page
};

#endif