#ifndef BPLUS_TREE_TUPLE_DEFINITIONS_H
#define BPLUS_TREE_TUPLE_DEFINITIONS_H

#include<tuple_def.h>

typedef struct bplus_tree_tuple_defs bplus_tree_tuple_defs;
struct bplus_tree_tuple_defs
{
	// page_id_with is bytes required for storing page_id, it can be 1,2,4 or 8
	int page_id_width;

	// size of each page inside this bplus tree
	uint32_t page_size;

	// this is what is considered as a NULL pointer in the bplus_tree
	uint64_t NULL_PAGE_ID;

	// number of elements considered as keys
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in record_def) in the order as you want them to be ordered
	const uint32_t* key_element_ids;

	// ith element_def in index_def has the same type, name and size as the key_element_ids[i] th element_def in record_def

	// tuple definition of the leaf pages in the bplus_tree
	const tuple_def* record_def;

	// tuple definition of the interior pages in the bplus_tree
	tuple_def* index_def;

	// tuple definition of the key to be used with this bplus_tree
	// for all of find, insert and delete functionalities
	tuple_def* key_def;
};

// initializes the attributes in bplus_tree_tuple_defs struct as per the provided parameters
// it allocates memory only for index_def
// returns 1 for success, it fails with 0, if the record_def has element_count 0 OR key_element_count == 0 OR key_elements == NULL OR if any of the key_element_ids is out of bounds
// page_id_with is bytes required for storing page_id, it can be 1,2,4 or 8
int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count, uint32_t page_size, int page_id_width, uint64_t NULL_PAGE_ID);

// it deallocates the index_def and
// then resets all the bplus_tree_tuple_defs struct attributes to NULL or 0
void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p);

#endif