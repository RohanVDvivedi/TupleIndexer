#include<bplus_tree_index_tuple_functions_util.h>

#include<tuple.h>

uint64_t get_child_page_id_from_index_tuple(const void* index_tuple, const bplus_tree_tuple_defs* bpttd_p)
{
	uint64_t child_page_id = bpttd_p->NULL_PAGE_ID;

	if(is_NULL_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple))
		return child_page_id;

	child_page_id = get_value_from_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple).uint_value;

	return child_page_id;
}

void set_child_page_id_in_index_tuple(void* index_tuple, uint64_t child_page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &((user_value){.uint_value = child_page_id}));
}