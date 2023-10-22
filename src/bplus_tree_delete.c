#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_page_header.h>
#include<sorted_packed_page_util.h>
#include<persistent_page_functions.h>
#include<bplus_tree_merge_util.h>

#include<stdlib.h>

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	uint32_t root_page_level;

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			return 0;

		// pre cache level of the root_page
		root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
			exit(-1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
	}

	// walk down taking locks until you reach leaf page level = 0
	walk_down_locking_parent_pages_for_merge_using_key(root_page_id, 0, locked_pages_stack_p, key, bpttd_p, dam_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// deleted will be set if the record, was deleted
	int deleted = 0;

	// this has to be a leaf page
	// we access it using pointer, so that upon deleting, its was modified bit get's set
	locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

	// if this check fails, then you are not using the utilities rightly
	if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
	{
		// find index of last record that has the given key on the page
		uint32_t found_index = find_last_in_sorted_packed_page(
											&(curr_locked_page->ppage), bpttd_p->page_size,
											bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
											key, bpttd_p->key_def, NULL
										);

		// if no such record can be found, we break and exit
		if(NO_TUPLE_FOUND == found_index)
			goto EXIT;

		// perform a delete operation on the found index in this page
		deleted = delete_in_sorted_packed_page(
							&(curr_locked_page->ppage), bpttd_p->page_size,
							bpttd_p->record_def,
							found_index,
							pmm_p,
							transaction_id,
							abort_error
						);

		if((*abort_error) || !deleted) // THIS IS AN ERR, WE CANT RECOVER FROM
		{
			// ABORT
			// release locks on all the pages, we had locks on until now
			goto EXIT;
		}
		else
		{
			merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
				return 0;
		}
	}
	else
	{
		// you must never arrive here
		goto EXIT;
	}

	EXIT:;
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	if(*abort_error)
		return 0;

	return deleted;
}