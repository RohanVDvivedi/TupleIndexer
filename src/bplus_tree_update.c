#include<bplus_tree.h>

#include<bplus_tree_split_insert_util.h>
#include<bplus_tree_merge_util.h>
#include<bplus_tree_find_util.h>
#include<persistent_page_functions.h>
#include<sorted_packed_page_util.h>

int inspected_update_in_bplus_tree(uint64_t root_page_id, void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
{
	if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
		return 0;
	
	persistent_page concerned_leaf = get_NULL_persistent_page(dam_p);
	read_couple_locks_until_leaf_using_record(root_page_id, new_record, bpttd_p->key_element_count, WRITE_LOCK, &concerned_leaf, bpttd_p, dam_p);

	// find index of last record that compared equal to the new_record
	uint32_t found_index = find_last_in_sorted_packed_page(
											&(concerned_leaf), bpttd_p->page_size,
											bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
											new_record, bpttd_p->record_def, bpttd_p->key_element_ids
										);

	// get the reference to the old_record
	const void* old_record = NULL;
	uint32_t old_record_size = 0;
	if(NO_TUPLE_FOUND != found_index)
	{
		old_record = get_nth_tuple_on_persistent_page(&concerned_leaf, bpttd_p->page_size, &(bpttd_p->record_def->size_def), found_index);
		old_record_size = get_tuple_size(bpttd_p->record_def, old_record);
	}

	if(ui_p->update_inspect(ui_p->context, bpttd_p->record_def, old_record, &new_record) == 0)
	{
		release_lock_on_persistent_page(dam_p, &concerned_leaf, NONE_OPTION);
		return 0;
	}

	// if old_record did not exist and the new_record is set to NULL (i.e. a request for deletion, the do nothing)
	if(old_record == NULL && new_record == NULL)
	{
		release_lock_on_persistent_page(dam_p, &concerned_leaf, NONE_OPTION);
		return 0;
	}
	else if(old_record == NULL && new_record != NULL) // insert case
	{
		// check again if the new_record is small enough to be inserted on the page
		if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
			return 0;
	}
	else if(old_record != NULL && new_record == NULL) // delete case
	{

	}
	else // update
	{
		// check again if the new_record is small enough to be inserted on the page
		if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
			return 0;

		// compute the size of the new record
		uint32_t new_record_size = get_tuple_size(bpttd_p->record_def, new_record);
	}
}