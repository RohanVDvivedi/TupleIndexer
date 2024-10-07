#include<bplus_tree.h>

#include<bplus_tree_iterator.h>

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int is_stacked, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(is_stacked)
		return get_new_bplus_tree_stacked_iterator(root_page_id, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);
	else
		return get_new_bplus_tree_unstacked_iterator(root_page_id, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);
}