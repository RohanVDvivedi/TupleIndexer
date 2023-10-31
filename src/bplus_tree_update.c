#include<bplus_tree.h>

#include<bplus_tree_split_insert_util.h>
#include<bplus_tree_merge_util.h>
#include<bplus_tree_find_for_update_util.h>
#include<bplus_tree_page_header.h>
#include<persistent_page_functions.h>
#include<sorted_packed_page_util.h>
#include<storage_capacity_page_util.h>

#include<stdlib.h>

// on an abort, lock on concerned leaf is released by the function
static int walk_down_and_split_insert_util(uint64_t root_page_id, persistent_page* concerned_leaf, const void* new_record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	if(concerned_leaf->page_id != root_page_id)
	{
		uint32_t root_page_level;

		{
			// get lock on the root page of the bplus_tree
			persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, concerned_leaf, NONE_OPTION, abort_error);
				return 0;
			}

			// pre cache level of the root_page
			root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

			// create a stack of capacity = levels
			if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
				exit(-1);

			// push the root page onto the stack
			push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
		}

		// walk down taking locks until you reach leaf page level = 0
		walk_down_locking_parent_pages_for_split_insert_using_record(root_page_id, 1, locked_pages_stack_p, new_record, bpttd_p, dam_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(dam_p, transaction_id, concerned_leaf, NONE_OPTION, abort_error);
			deinitialize_locked_pages_stack(locked_pages_stack_p);
			return 0;
		}
	}
	else
	{
		// else concerned_leaf is the root page, all we need to do is put it in the locked pages stack, i.e. require stack with capacity 2 (one for leaf page and its parent if it splits)
		if(!initialize_locked_pages_stack(locked_pages_stack_p, 3))
			exit(-1);
	}

	// push the concerned_leaf to the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(*concerned_leaf));

	// perform split insert
	int inserted = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, new_record, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return 0;
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return inserted;
}

// concerned_leaf is expected to not be the root_page
// root_pages can not merge
// on an abort, lock on concerned leaf is released by the function
static int walk_down_and_merge_util(uint64_t root_page_id, persistent_page* concerned_leaf, const void* old_record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	uint32_t root_page_level;

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(dam_p, transaction_id, concerned_leaf, NONE_OPTION, abort_error);
			return 0;
		}

		// pre cache level of the root_page
		root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
			exit(-1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
	}

	walk_down_locking_parent_pages_for_merge_using_record(root_page_id, 1, locked_pages_stack_p, old_record, bpttd_p, dam_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(dam_p, transaction_id, concerned_leaf, NONE_OPTION, abort_error);
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return 0;
	}

	// push the concerned_leaf to the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(*concerned_leaf));

	merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return 0;
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return 1;
}

int inspected_update_in_bplus_tree(uint64_t root_page_id, void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
		return 0;

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

	uint32_t release_for_split = 0;
	uint32_t release_for_merge = 0;
	walk_down_locking_parent_pages_for_update_using_record(root_page_id, locked_pages_stack_p, new_record, &release_for_split, &release_for_merge, bpttd_p, dam_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// concerned_leaf will always be at the top of this stack
	persistent_page concerned_leaf = &(get_top_of_locked_pages_stack(locked_pages_stack_p)->ppage);

	// find index of last record that compares equal to the new_record
	uint32_t found_index = find_last_in_sorted_packed_page(
											&(concerned_leaf), bpttd_p->page_size,
											bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
											new_record, bpttd_p->record_def, bpttd_p->key_element_ids
										);

	// get the reference to the old_record
	void* old_record = NULL;
	uint32_t old_record_size = 0;
	if(NO_TUPLE_FOUND != found_index)
	{
		old_record = (void*) get_nth_tuple_on_persistent_page(&concerned_leaf, bpttd_p->page_size, &(bpttd_p->record_def->size_def), found_index);
		old_record_size = get_tuple_size(bpttd_p->record_def, old_record);
	}

	// result to return
	int result = 0;

	int ui_res = ui_p->update_inspect(ui_p->context, bpttd_p->record_def, old_record, &new_record, transaction_id, abort_error);
	if((*abort_error) || ui_res == 0)
	{
		result = 0;
		goto ABORT_OR_FAIL;
	}

	// if old_record did not exist and the new_record is set to NULL (i.e. a request for deletion, then do nothing)
	if(old_record == NULL && new_record == NULL)
	{
		result = 1;
		goto ABORT_OR_FAIL;
	}
	else if(old_record == NULL && new_record != NULL) // insert case
	{
		// check again if the new_record is small enough to be inserted on the page
		if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
		{
			result = 0;
			goto ABORT_OR_FAIL;
		}

		// we can release lock on release_for_split number of parent pages
		while(release_for_split > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto ABORT_OR_FAIL;

			release_for_split--;
		}

		result = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, new_record, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_OR_FAIL;

		// release allocation for locked_pages_stack
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return result;
	}
	else if(old_record != NULL && new_record == NULL) // delete case
	{
		// we can release lock on release_for_merge number of parent pages
		while(release_for_merge > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto ABORT_OR_FAIL;

			release_for_merge--;
		}

		// perform a delete operation on the found index in this page
		result = delete_in_sorted_packed_page(
							&(concerned_leaf), bpttd_p->page_size,
							bpttd_p->record_def,
							found_index,
							pmm_p,
							transaction_id,
							abort_error
						);
		if(*abort_error)
			goto ABORT_OR_FAIL;

		merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_OR_FAIL;

		// release allocation for locked_pages_stack
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return result;
	}
	else // update
	{
		// fail if the keys of old_record and new_record, do not match then quit
		if(0 != compare_tuples(old_record, bpttd_p->record_def, bpttd_p->key_element_ids, new_record, bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count))
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;
			return 0;
		}

		// check again if the new_record is small enough to be inserted on the page
		if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, new_record))
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;
			return 0;
		}

		// compute the size of the new record
		uint32_t new_record_size = get_tuple_size(bpttd_p->record_def, new_record);

		int updated = update_at_in_sorted_packed_page(
									&concerned_leaf, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									new_record, 
									found_index,
									pmm_p,
									transaction_id,
									abort_error
								);
		if(*abort_error)
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
			return 0;
		}

		if(new_record_size != old_record_size) // inplace update
		{
			if(new_record_size > old_record_size) // may require split
			{
				if(!updated) // then this leaf must split
				{
					// first perform a delete and then a split insert
					// this function may not fail, because our found index is valid
					delete_in_sorted_packed_page(
							&(concerned_leaf), bpttd_p->page_size,
							bpttd_p->record_def,
							found_index,
							pmm_p,
							transaction_id,
							abort_error
						);
					if(*abort_error)
					{
						release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
						return 0;
					}

					updated = walk_down_and_split_insert_util(root_page_id, &concerned_leaf, new_record, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
					if(*abort_error)
						return 0;
				}
				else
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
					if(*abort_error)
						return 0;
				}
			}
			else // new_record_size < old_record_size -> may require merge -> but it is assumed to be true that updated = 1
			{
				if(concerned_leaf.page_id != root_page_id && is_page_lesser_than_or_equal_to_half_full(&concerned_leaf, bpttd_p->page_size, bpttd_p->record_def))
				{
					// new_record has the same key contents as the old_record, hence we can merge at the position corresponding to the new_record
					walk_down_and_merge_util(root_page_id, &concerned_leaf, new_record, bpttd_p, dam_p, pmm_p, transaction_id, abort_error);
					if(*abort_error)
						return 0;
				}
				else
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
					if(*abort_error)
						return 0;
				}
			}
		}
		else
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &concerned_leaf, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;
		}

		return updated;
	}


	ABORT_OR_FAIL:;
	// release all locks of the locked_pages_stack and exit
	// do not worry about abort_error here or in the loop, as we are only releasing locks
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// release allocation for locked_pages_stack
	deinitialize_locked_pages_stack(locked_pages_stack_p);

	// on an abort, overide the result to 0
	if(*abort_error)
		result = 0;

	return result;
}