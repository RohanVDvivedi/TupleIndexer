#ifndef BPLUS_TREE_UTIL_H
#define BPLUS_TREE_UTIL_H

// two of the page types found inside a bplus tree
#define LEAF_PAGE_TYPE      0
#define INTERIOR_PAGE_TYPE  1

// page reference on interior page
// i.e. page reference to the sub-bplus_tree that contains all data lesser than the least key on this page
#define ALL_LEAST_REF       0

// page references on leaf page
#define NEXT_PAGE_REF       0
#define PREV_PAGE_REF       1

typedef struct bplus_tree_tuple_defs bplus_tree_tuple_defs;
struct bplus_tree_tuple_defs
{
	// tuple definition to compare keys in the bplus_tree
	tuple_def* key_def;

	// tuple definition of the interior pages in the bplus_tree
	// this is equal to { key_def : page_id(u4)) }
	tuple_def* index_def;

	// tuple definition of the leaf pages in the bplus_tree
	// this is equal to { key_def : value_def }
	tuple_def* record_def;
};

// you may generate tuple definitions before performing any operations
const bplus_tree_tuple_defs* get_bplus_tree_tuple_defs(tuple_def* record_def, uint16_t key_element_count);

// and then discard when your work with bplus tree is completed
void del_bplus_tree_tuple_defs(bplus_tree_tuple_defs* bpttds);

#endif