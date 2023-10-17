#include<bplus_tree.h>

#include<bplus_tree_split_insert_util.h>
#include<bplus_tree_merge_util.h>
#include<bplus_tree_find_util.h>

int inspected_update_in_bplus_tree(uint64_t root_page_id, const void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
{
	if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
		return 0;
	// TODO
}