#ifndef BPLUS_TREE_TUPLE_DEFINITIONS_H
#define BPLUS_TREE_TUPLE_DEFINITIONS_H

#include<tuple_def.h>

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

void init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const tuple_def* record_def, uint32_t key_element_count, uint32_t page_size);

void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p);

#endif