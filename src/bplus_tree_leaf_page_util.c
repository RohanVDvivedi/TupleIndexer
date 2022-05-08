#include<bplus_tree_leaf_page_util.h>

#include<sorted_packed_page_util.h>

uint32_t find_greater_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_succeeding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

uint32_t find_lesser_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

const void* split_insert_bplus_tree_leaf_page(locked_page_info* page_info, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{

}

int merge_bplus_tree_leaf_pages(locked_page_info* page_info1, locked_page_info* page_info2, bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{

}