#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include<stdint.h>

#include<tuple.h>

#include<data_access_methods.h>

typedef struct bplus_tree_index bplus_tree_index;
struct bplus_tree_index
{
	// the root page of the bplus_tree
	uint32_t root_page;

	// the tuple definition of the data at the leaf page
	const tuple_def* tpl_d;

	// the number of elements from the beginning, that will be considered for comparison
	// i.e. the prefix elements of the tuple will be considered as key, and the rest will be considered as value
	uint16_t key_elements_count;

	// only the leaf pages will store key and value
	// the internal pages and the root page will store the key vs the page_id (for indirection to other internal pages or the leaf pages)
};

const void* find_in_bplus_tree_index(const bplus_tree_index* bti_p, const void* key_tuple, const data_access_methods* dam_p);

int insert_in_bplus_tree_index(bplus_tree_index* bti_p, const void* key_value_tuple, const data_access_methods* dam_p);

int delete_in_bplus_tree_index(bplus_tree_index* bti_p, const void* key_tuple, const data_access_methods* dam_p);

#endif