#include<tupleindexer/bplus_tree/bplus_tree_index_tuple_functions_util.h>

#include<tuplestore/tuple.h>

// index_def will always have (key_element_count + 1) elements
// and the last element of any of it's tuple (i.e. the one at index key_element_count) will always be the child page id

uint64_t get_child_page_id_from_index_tuple(const void* index_tuple, const bplus_tree_tuple_defs* bpttd_p)
{
	// get user value for the child_page_id element from the tuple
	// we neet not worry, this element in the tuple will not be NULL, check bplus_tree_tuple_defs.c for more information
	user_value uval;
	get_value_from_element_from_tuple(&uval, bpttd_p->index_def, STATIC_POSITION(bpttd_p->key_element_count), index_tuple);
	return uval.uint_value;
}

void set_child_page_id_in_index_tuple(void* index_tuple, uint64_t child_page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	set_element_in_tuple(bpttd_p->index_def, STATIC_POSITION(bpttd_p->key_element_count), index_tuple, &((const user_value){.uint_value = child_page_id}), UINT32_MAX);
}