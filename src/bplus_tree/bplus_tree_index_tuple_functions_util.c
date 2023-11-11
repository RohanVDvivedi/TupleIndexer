#include<bplus_tree_index_tuple_functions_util.h>

#include<tuple.h>

uint64_t get_child_page_id_from_index_tuple(const void* index_tuple, const bplus_tree_tuple_defs* bpttd_p)
{
	// get user value for the child_page_id element from the tuple
	// we neet not worry, this element in the tuple will not be NULL, check bplus_tree_tuple_defs.c for more information
	return get_value_from_element_from_tuple(bpttd_p->index_def, get_element_def_count_tuple_def(bpttd_p->index_def) - 1, index_tuple).uint_value;
}

void set_child_page_id_in_index_tuple(void* index_tuple, uint64_t child_page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	set_element_in_tuple(bpttd_p->index_def, get_element_def_count_tuple_def(bpttd_p->index_def) - 1, index_tuple, &((const user_value){.uint_value = child_page_id}));
}