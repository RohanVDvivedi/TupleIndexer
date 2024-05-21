#include<bplus_tree.h>

#include<bplus_tree_split_insert_util.h>
#include<bplus_tree_merge_util.h>
#include<bplus_tree_find_for_update_util.h>
#include<bplus_tree_page_header.h>
#include<persistent_page_functions.h>
#include<sorted_packed_page_util.h>
#include<storage_capacity_page_util.h>

#include<stdlib.h>

typedef struct cancel_update_callback_context cancel_update_callback_context;
struct cancel_update_callback_context
{
	// set to true if cancel_update_callback gets called
	int update_canceled;
	locked_pages_stack* locked_pages_stack_p;
	const page_access_methods* pam_p;
};

void cancel_update_callback(void* cancel_update_callback_context_p, const void* transaction_id, int* abort_error)
{
	cancel_update_callback_context* cuc_cntxt = cancel_update_callback_context_p;

	cuc_cntxt->update_canceled = 1;

	while(get_element_count_locked_pages_stack(cuc_cntxt->locked_pages_stack_p) > 1)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(cuc_cntxt->locked_pages_stack_p);
		release_lock_on_persistent_page(cuc_cntxt->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(cuc_cntxt->locked_pages_stack_p);

		// on abort_error, return to the caller
		if(*abort_error)
			return;
	}

	// if you reach here then there is only 1 page locked, and that must be leaf page that we are concerned about

	// we just downgrade that lock and return to the user, we do not pop it, since we will still be holding read lock on it, as long as the update_inspector will read the old_record
	locked_page_info* bottom = get_bottom_of_locked_pages_stack(cuc_cntxt->locked_pages_stack_p);
	downgrade_to_reader_lock_on_persistent_page(cuc_cntxt->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
}

int inspected_update_in_bplus_tree(uint64_t root_page_id, void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(!check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(bpttd_p, new_record))
		return 0;

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			return 0;

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
			exit(-1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, INVALID_TUPLE_INDEX));
	}

	uint32_t release_for_split = 0;
	uint32_t release_for_merge = 0;
	walk_down_locking_parent_pages_for_update_using_record(root_page_id, locked_pages_stack_p, new_record, &release_for_split, &release_for_merge, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
	{
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return 0;
	}

	// concerned_leaf will always be at the top of this stack
	persistent_page* concerned_leaf = &(get_top_of_locked_pages_stack(locked_pages_stack_p)->ppage);

	// find index of last record that compares equal to the new_record
	uint32_t found_index = find_last_in_sorted_packed_page(
											concerned_leaf, bpttd_p->pas_p->page_size,
											bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
											new_record, bpttd_p->record_def, bpttd_p->key_element_ids
										);

	// get the reference to the old_record
	void* old_record = NULL;
	uint32_t old_record_size = 0;
	if(NO_TUPLE_FOUND != found_index)
	{
		old_record = (void*) get_nth_tuple_on_persistent_page(concerned_leaf, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), found_index);
		old_record_size = get_tuple_size(bpttd_p->record_def, old_record);
	}

	// result to return
	int result = 0;

	// OPTIMIZATION - release early locks of release_for_split number of parent pages, if you can anticipate that you may not be merging
	// if old_record is NULL, then all you can do is an insert (consequently a split insert)
	// you can't update or delete a non existent record, you can only insert into its place, so you may end up splitting, but never merging
	if(old_record == NULL)
	{
		while(release_for_split > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

			release_for_split--;
		}
	}
	// as you could guess, you can discard this piece of code
	// OPTIMIZATION - end

	{
		// extarct key from the new_record prior to the update_inspect
		char* new_record_key = malloc(bpttd_p->max_index_record_size); // key will never be bigger than the largest index_record
		if(new_record_key == NULL)
			exit(-1);
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(bpttd_p, new_record, new_record_key);

		cancel_update_callback_context cuc_cntxt = {.update_canceled = 0, .locked_pages_stack_p = locked_pages_stack_p, .pam_p = pam_p};
		int ui_res = ui_p->update_inspect(ui_p->context, bpttd_p->record_def, old_record, &new_record, cancel_update_callback, &cuc_cntxt, transaction_id, abort_error);
		if((*abort_error) || cuc_cntxt.update_canceled == 1 || ui_res == 0)
		{
			free(new_record_key);
			result = 0;
			goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
		}

		// if the new_record is not NULL (insert or update case), then the below checks must pass
		if(new_record != NULL)
		{
			// compare key elements of new_record and new_record_key, to be sure that the key elements of the new_record were not changed
			int key_elements_compare = compare_tuples(new_record, bpttd_p->record_def, bpttd_p->key_element_ids, new_record_key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count);
			free(new_record_key);

			// fail if key elements of the new_record were changed
			if(key_elements_compare)
			{
				result = 0;
				goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
			}

			// check again if the new_record is small enough to be inserted on the page
			if(!check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(bpttd_p, new_record))
			{
				result = 0;
				goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
			}
		}
		else
			free(new_record_key);
	}

	// if old_record did not exist and the new_record is set to NULL (i.e. a request for deletion, then do nothing)
	if(old_record == NULL && new_record == NULL)
	{
		result = 0;
		goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
	}
	else if(old_record == NULL && new_record != NULL) // insert case
	{
		// we can release lock on release_for_split number of parent pages
		while(release_for_split > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

			release_for_split--;
		}

		result = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, new_record, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);

		// deinitialize stack, all page locks will be released, after return of the above function
		deinitialize_locked_pages_stack(locked_pages_stack_p);

		// on abort, result will be 0
		return result;
	}
	else if(old_record != NULL && new_record == NULL) // delete case
	{
		// we can release lock on release_for_merge number of parent pages
		while(release_for_merge > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

			release_for_merge--;
		}

		// perform a delete operation on the found index in this page, this has to succeed
		delete_in_sorted_packed_page(
							concerned_leaf, bpttd_p->pas_p->page_size,
							bpttd_p->record_def,
							found_index,
							pmm_p,
							transaction_id,
							abort_error
						);
		if(*abort_error)
			goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

		result = merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);

		// deinitialize stack, all page locks will be released, after return of the above function
		deinitialize_locked_pages_stack(locked_pages_stack_p);

		// on abort, result will be 0
		return result;
	}
	else // update
	{
		// compute the size of the new record
		uint32_t new_record_size = get_tuple_size(bpttd_p->record_def, new_record);

		int updated = update_at_in_sorted_packed_page(
									concerned_leaf, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									new_record, 
									found_index,
									pmm_p,
									transaction_id,
									abort_error
								);
		if(*abort_error)
			goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

		if(new_record_size != old_record_size) // inplace update
		{
			if(new_record_size > old_record_size) // may require split
			{
				if(!updated) // then this leaf must split
				{
					// first perform a delete and then a split insert
					// this function may not fail, because our found index is valid
					delete_in_sorted_packed_page(
							concerned_leaf, bpttd_p->pas_p->page_size,
							bpttd_p->record_def,
							found_index,
							pmm_p,
							transaction_id,
							abort_error
						);
					if(*abort_error)
						goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

					// we can release lock on release_for_split number of parent pages
					while(release_for_split > 0)
					{
						locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
						release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
						pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

						if(*abort_error)
							goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

						release_for_split--;
					}

					// once the delete is done, we can split insert the new_record
					result = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, new_record, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);

					// deinitialize stack, all page locks will be released, after return of the above function
					deinitialize_locked_pages_stack(locked_pages_stack_p);

					// on abort, result will be 0
					return result;
				}
				else
				{
					// the record was big, yet we could fit it on the page, nothing to be done really
					result = 1;
					goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
				}
			}
			else // new_record_size <= old_record_size -> may require merge -> but it is assumed to be true that updated = 1
			{
				// we can release lock on release_for_merge number of parent pages
				while(release_for_merge > 0)
				{
					locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
					release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
					pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

					if(*abort_error)
						goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;

					release_for_merge--;
				}

				result = merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);

				// deinitialize stack, all page locks will be released, after return of the above function
				deinitialize_locked_pages_stack(locked_pages_stack_p);

				// on abort, result will be 0
				return result;
			}
		}
		else // we do not need to do any thing further, if the records are of same size
		{
			// set return value to 1, and RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT will take care of the rest
			result = 1;
			goto RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT;
		}
	}


	RELEASE_LOCKS_DEINITIALIZE_STACK_AND_EXIT:;
	// release all locks of the locked_pages_stack and exit
	// do not worry about abort_error here or in the loop, as we are only releasing locks
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// release allocation for locked_pages_stack
	deinitialize_locked_pages_stack(locked_pages_stack_p);

	// on an abort, overide the result to 0
	if(*abort_error)
		result = 0;

	return result;
}