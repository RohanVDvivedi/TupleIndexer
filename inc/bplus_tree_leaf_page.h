#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	uint32_t left_link;		// towards all lesser data
	uint32_t right_link;	// towards all greater data
};

#endif