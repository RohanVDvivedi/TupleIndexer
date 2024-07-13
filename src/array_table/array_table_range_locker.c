#include<array_table_range_locker.h>

#include<persistent_page_functions.h>
#include<array_table_page_util.h>
#include<array_table_page_header.h>
#include<locked_pages_stack.h>
#include<invalid_tuple_indices.h>

#include<stdlib.h>

array_table_range_locker* get_new_array_table_range_locker(uint64_t root_page_id, bucket_range lock_range, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// fail if the lock range is invalid
	if(!is_valid_bucket_range(&lock_range))
		return NULL;

	// the following 2 must be present
	if(attd_p == NULL || pam_p == NULL)
		return NULL;

	array_table_range_locker* atrl_p = malloc(sizeof(array_table_range_locker));
	if(atrl_p == NULL)
		exit(-1);

	// start initializing the atrl, making it point to and lock the actual root of the array_table
	atrl_p->delegated_local_root_range = WHOLE_BUCKET_RANGE;
	atrl_p->max_local_root_level = attd_p->max_array_table_height - 1;
	atrl_p->local_root = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, ((pmm_p == NULL) ? READ_LOCK : WRITE_LOCK), abort_error);
	if(*abort_error)
	{
		free(atrl_p);
		return NULL;
	}
	atrl_p->root_page_id = root_page_id;
	atrl_p->attd_p = attd_p;
	atrl_p->pam_p = pam_p;
	atrl_p->pmm_p = pmm_p;

	// minimize the lock range, from [0,UINT64_MAX] to lock_range
	minimize_lock_range_for_array_table_range_locker(atrl_p, lock_range, transaction_id, abort_error);
	if(*abort_error)
	{
		free(atrl_p);
		return NULL;
	}

	return atrl_p;
}

int minimize_lock_range_for_array_table_range_locker(array_table_range_locker* atrl_p, bucket_range lock_range, const void* transaction_id, int* abort_error)
{
	// fail, if lock_range is invalid OR if the lock_range is not contained within the delegated range of the local_root
	if(!is_valid_bucket_range(&lock_range) || !is_contained_bucket_range(&(atrl_p->delegated_local_root_range), &lock_range))
		return 0;

	while(1)
	{
		// if the local_root is already a leaf page then quit
		if(is_array_table_leaf_page(&(atrl_p->local_root), atrl_p->attd_p))
			break;

		bucket_range actual_range = get_bucket_range_for_array_table_page(&(atrl_p->local_root), atrl_p->attd_p);

		// if the actual_range is not contained within the lock_range provided then quit
		// no child to go down further into
		if(!is_contained_bucket_range(&actual_range, &lock_range))
			break;

		uint32_t first_bucket_child_index = get_child_index_for_bucket_id_on_array_table_page(&(atrl_p->local_root), lock_range.first_bucket_id, atrl_p->attd_p);
		uint32_t last_bucket_child_index = get_child_index_for_bucket_id_on_array_table_page(&(atrl_p->local_root), lock_range.last_bucket_id, atrl_p->attd_p);

		// the lock_range may point to a single child on the local_root, if so we can go down further narrowing the lock range
		if(first_bucket_child_index != last_bucket_child_index)
			break;

		// this is the child we must go to
		// its delegated_range contains the lock_range
		uint32_t child_index = first_bucket_child_index;

		// get child_page_id to go to
		uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_index_page(&(atrl_p->local_root), child_index, atrl_p->attd_p);

		// if this entry does not point to an existing child, then we can not make it the local root
		if(child_page_id == atrl_p->attd_p->pas_p->NULL_PAGE_ID)
			break;

		// update the local_root's delegated_range and max_level
		atrl_p->delegated_local_root_range = get_delegated_bucket_range_for_child_index_on_array_table_page(&(atrl_p->local_root), child_index, atrl_p->attd_p);
		atrl_p->max_local_root_level = get_level_of_array_table_page(&(atrl_p->local_root), atrl_p->attd_p) - 1;

		// acquire lock on new_local_root, then release lock on old local_root
		persistent_page new_local_root = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, (is_writable_array_table_range_locker(atrl_p) ? WRITE_LOCK : READ_LOCK), abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
			return 0;
		}
		release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &new_local_root, NONE_OPTION, abort_error);
			return 0;
		}

		// update the new local_root
		atrl_p->local_root = new_local_root;
	}

	return 1;
}

bucket_range get_lock_range_for_array_table_range_locker(const array_table_range_locker* atrl_p)
{
	return atrl_p->delegated_local_root_range;
}

int is_writable_array_table_range_locker(const array_table_range_locker* atrl_p)
{
	// the range_locker is writable if we have a non-null pmm_p
	return atrl_p->pmm_p != NULL;
}

// release lock on the persistent_page, and ensure that local_root is not unlocked
// only used in get and set functions (and find_non_NULL_in_array_table)
static void release_lock_on_persistent_page_while_preventing_local_root_unlocking(persistent_page* ppage, array_table_range_locker* atrl_p, const void* transaction_id, int* abort_error)
{
	// if it is the local_root you are releasing lock on, then you only need to copy it back to local_root of atrl
	if(ppage->page_id == atrl_p->local_root.page_id)
	{
		atrl_p->local_root = (*ppage);
		(*ppage) = get_NULL_persistent_page(atrl_p->pam_p);
		return;
	}
	release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, ppage, NONE_OPTION, abort_error);
}

const void* get_from_array_table(array_table_range_locker* atrl_p, uint64_t bucket_id, void* preallocated_memory, const void* transaction_id, int* abort_error)
{
	// fail if the bucket_id is not contained within the delegated range of the local_root
	if(!is_bucket_contained_bucket_range(&(atrl_p->delegated_local_root_range), bucket_id))
		return NULL;

	persistent_page curr_page = atrl_p->local_root;
	while(1)
	{
		// if the curr_page has been locked, then its delegated range will and must contain the bucket_id

		// actual range of curr_page
		bucket_range curr_page_actual_range = get_bucket_range_for_array_table_page(&curr_page, atrl_p->attd_p);

		// if bucket_id is not contained in the actual_range of the curr_page, then return NULL_PAGE_ID
		if(!is_bucket_contained_bucket_range(&curr_page_actual_range, bucket_id))
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return NULL;
		}

		// get child_page_id to goto next
		uint32_t child_index = get_child_index_for_bucket_id_on_array_table_page(&curr_page, bucket_id, atrl_p->attd_p);

		// if this concerned child is NULL, then return NULL
		if(is_NULL_at_child_index_in_array_table_page(&curr_page, child_index, atrl_p->attd_p))
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return NULL;
		}

		// if it is a leaf page, then we return the record_entry at the child_index
		if(is_array_table_leaf_page(&curr_page, atrl_p->attd_p))
		{
			const void* record_found = get_record_entry_at_child_index_in_array_table_leaf_page(&curr_page, child_index, preallocated_memory, atrl_p->attd_p);
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return record_found;
		}

		// now this next child is surely not NULL, it is the one we will go to
		uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_index_page(&curr_page, child_index, atrl_p->attd_p);

		// we need to READ_LOCK the child_page, then release lock on the curr_page
		persistent_page child_page = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
			goto ABORT_ERROR;
		}
		release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&child_page, atrl_p, transaction_id, abort_error);
			goto ABORT_ERROR;
		}

		// now make child_page as the curr_page and loop over
		curr_page = child_page;
	}

	// you will never come here
	return NULL;

	ABORT_ERROR:;
	// release lock on local root
	release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
	return NULL;
}

int set_in_array_table(array_table_range_locker* atrl_p, uint64_t bucket_id, const void* record, const void* transaction_id, int* abort_error)
{
	// we cannot set if the atrl is not locked for reading
	if(!is_writable_array_table_range_locker(atrl_p))
		return 0;

	// fail if the bucket_id is not contained within the delegated range of the local_root
	if(!is_bucket_contained_bucket_range(&(atrl_p->delegated_local_root_range), bucket_id))
		return 0;

	if(record != NULL) // there is difference in how we set, based on whether record is NULL or non-NULL
	{
		persistent_page curr_page = atrl_p->local_root;
		while(1)
		{
			// if the curr_page has been locked, then its delegated range will and must contain the bucket_id

			// if the page we are looking at is empty, then re-purpose the page as a new leaf page that contains the bucket_id
			if(has_all_NULL_entries_in_array_table_page(&curr_page, atrl_p->attd_p))
			{
				array_table_page_header hdr = get_array_table_page_header(&curr_page, atrl_p->attd_p);
				hdr.level = 0;
				hdr.first_bucket_id = get_first_bucket_id_for_level_containing_bucket_id_for_array_table_page(0, bucket_id, atrl_p->attd_p);
				init_array_table_page(&curr_page, hdr.level, hdr.first_bucket_id, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				uint32_t child_index = get_child_index_for_bucket_id_on_array_table_page(&curr_page, bucket_id, atrl_p->attd_p);
				set_record_entry_at_child_index_in_array_table_leaf_page(&curr_page, child_index, record, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				return 1;
			}

			// actual range of curr_page
			bucket_range curr_page_actual_range = get_bucket_range_for_array_table_page(&curr_page, atrl_p->attd_p);

			// if bucket_id is not contained in the actual_range of the curr_page, then level_up the page
			if(!is_bucket_contained_bucket_range(&curr_page_actual_range, bucket_id))
			{
				level_up_array_table_page(&curr_page, atrl_p->attd_p, atrl_p->pam_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}
				continue;
			}

			// now the page must have its actual_range contain the bucket_id,
			// hence a child_index must exist
			uint32_t child_index = get_child_index_for_bucket_id_on_array_table_page(&curr_page, bucket_id, atrl_p->attd_p);

			// if it is leaf page, then all we need to do is insert the new_entry, bucket_id->page_id, and we are done
			if(is_array_table_leaf_page(&curr_page, atrl_p->attd_p))
			{
				set_record_entry_at_child_index_in_array_table_leaf_page(&curr_page, child_index, record, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
				return 1;
			}

			// for an interior page, fetch the child_page_id that we must follow next
			uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_index_page(&curr_page, child_index, atrl_p->attd_p);

			// if that child_page_id is NULL, then insert a new leaf in the curr_page
			if(child_page_id == atrl_p->attd_p->pas_p->NULL_PAGE_ID)
			{
				persistent_page child_page = get_new_persistent_page_with_write_lock(atrl_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				set_child_page_id_at_child_index_in_array_table_index_page(&curr_page, child_index, child_page.page_id, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&child_page, atrl_p, transaction_id, abort_error);
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&child_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				curr_page = child_page;

				// initialize this new child page as if it is an empty random page
				// it will be repurposed in the next iteration (here it is wrong to set its level and first_bucket_id both as zero, but it is anyway going to be repurposed in the next iteration)
				init_array_table_page(&curr_page, 0, 0, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				continue;
			}
			else // else if child_page_id != NULL_PAGE_ID, then go to this child and make it the curr_page
			{
				// we need to WRITE_LOCK the child_page, then release lock on the curr_page
				persistent_page child_page = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}
				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, atrl_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&child_page, atrl_p, transaction_id, abort_error);
					goto ABORT_ERROR;
				}

				// now make child_page as the curr_page and loop over
				curr_page = child_page;
			}
		}
	}
	else // else we have to set bucket_id -> NULL
	{
		// create a stack of capacity = levels
		locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
		if(!initialize_locked_pages_stack(locked_pages_stack_p, atrl_p->max_local_root_level + 1))
			exit(-1);

		// result to be returned
		int result = 0;

		// this will be set if reverse pass is required
		int reverse_pass_required = 0;

		// push local root onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(atrl_p->local_root, INVALID_TUPLE_INDEX));

		// forward pass walking down the array_table
		while(1)
		{
			locked_page_info* curr_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

			// actual range of curr_page
			bucket_range curr_page_actual_range = get_bucket_range_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p);

			// if bucket_id is not contained in the actual_range of the curr_page, then the record at bucket_id is already NULL
			if(!is_bucket_contained_bucket_range(&curr_page_actual_range, bucket_id))
			{
				result = 1;
				break;
			}

			// now the page must have its actual_range contain the bucket_id,
			// hence a child_index must exist
			curr_page->child_index = get_child_index_for_bucket_id_on_array_table_page(&(curr_page->ppage), bucket_id, atrl_p->attd_p);

			// if the entry at the child_index in the curr_page is already NULL, then nothing needs to be done
			if(is_NULL_at_child_index_in_array_table_page(&(curr_page->ppage), curr_page->child_index, atrl_p->attd_p))
			{
				result = 1;
				break;
			}

			// if we are at the leaf page, then set the entry to NULL, and break out
			if(is_array_table_leaf_page(&(curr_page->ppage), atrl_p->attd_p))
			{
				set_record_entry_at_child_index_in_array_table_leaf_page(&(curr_page->ppage), curr_page->child_index, NULL, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
				reverse_pass_required = 1;
				result = 1;
				break;
			}

			// else this i the child we need to go to
			uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_index_page(&(curr_page->ppage), curr_page->child_index, atrl_p->attd_p);

			// OPTIMIZATION
			// below piece of code releases locks on all parents of curr_page, if the curr_page will never become empty, even after NULL-ing the child_index on that page
			// this is an optimization to release early locks, you can remove this part and the functionality will still be identical 
			if(get_non_NULL_entry_count_in_array_table_page(&(curr_page->ppage), atrl_p->attd_p) > 1) // this page will exist even if one of its child is NULLed, so we can release lock on its parents
			{
				while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the curr_page)
				{
					locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(bottom->ppage), atrl_p, transaction_id, abort_error);
					pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

					if(*abort_error)
						goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
				}
			}
			// OPTIMIZATION ---x---

			// grab write lock on child page, push it onto the stack and then continue
			persistent_page child_page = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);
			if(*abort_error)
				goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
			push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
		}

		// if reverse pass is required then do it
		if(reverse_pass_required == 1)
		{
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
			{
				locked_page_info curr_page = *get_top_of_locked_pages_stack(locked_pages_stack_p);
				pop_from_locked_pages_stack(locked_pages_stack_p);

				uint32_t child_entry_count_curr_page = get_non_NULL_entry_count_in_array_table_page(&(curr_page.ppage), atrl_p->attd_p);

				if(child_entry_count_curr_page == 0)
				{
					if(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0) // then curr_page can not be the local_root, hence we can free it
					{
						// free the curr_page
						release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(curr_page.ppage), FREE_PAGE, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
							goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
						}

						// set NULL_PAGE_ID to its parent page entry
						locked_page_info* parent_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
						set_child_page_id_at_child_index_in_array_table_index_page(&(parent_page->ppage), parent_page->child_index, atrl_p->attd_p->pas_p->NULL_PAGE_ID, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
						if(*abort_error)
							goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
					}
					else // this page may be a local_root, so we can't free it
					{
						// write 0 to the level of the curr_page
						array_table_page_header hdr = get_array_table_page_header(&(curr_page.ppage), atrl_p->attd_p);
						hdr.level = 0;
						set_array_table_page_header(&(curr_page.ppage), &hdr, atrl_p->attd_p, atrl_p->pmm_p, transaction_id, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page.ppage), atrl_p, transaction_id, abort_error);
							goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
						}

						// release lock on curr_page
						release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page.ppage), atrl_p, transaction_id, abort_error);
						if(*abort_error)
							goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
					}
				}
				else if(child_entry_count_curr_page == 1 && !is_array_table_leaf_page(&(curr_page.ppage), atrl_p->attd_p)) // if you can level it down then do it
				{
					// level down curr_page
					level_down_array_table_page(&(curr_page.ppage), atrl_p->attd_p, atrl_p->pam_p, atrl_p->pmm_p, transaction_id, abort_error);
					if(*abort_error)
					{
						release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page.ppage), atrl_p, transaction_id, abort_error);
						goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
					}

					// push curr_page back into the stack
					push_to_locked_pages_stack(locked_pages_stack_p, &curr_page);
				}
				else
				{
					// release lock on curr_page
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page.ppage), atrl_p, transaction_id, abort_error);
					if(*abort_error)
						goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;

					break;
				}
			}
		}

		// release locks bottom first
		while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(bottom->ppage), atrl_p, transaction_id, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR;
		}

		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return result;

		RELEASE_LOCKS_FROM_STACK_ON_ABORT_ERROR:;
		// release all locks from stack bottom first
		while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(bottom->ppage), atrl_p, transaction_id, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
		}
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		goto ABORT_ERROR;
	}

	ABORT_ERROR:
	// release lock on local root
	release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
	return 0;
}

const void* find_non_NULL_entry_in_array_table(array_table_range_locker* atrl_p, uint64_t* bucket_id, void* preallocated_memory, find_position find_pos, const void* transaction_id, int* abort_error)
{
	// This function makes use of the fact that (leaf or index)_entries_per_page != INVALID_INDEX (UINT32_MAX)
	// i.e. (leaf or index)_entries_per_page < INVALID_INDEX (UINT32_MAX)

	// convert find_pos from LESSER_THAN to LESSER_THAN_EQUALS and GREATER_THAN to GREATER_THAN_EQUALS
	if(find_pos == LESSER_THAN)
	{
		if((*bucket_id) == 0)
			return NULL;
		find_pos = LESSER_THAN_EQUALS;
		(*bucket_id) -= 1;
	}
	else if(find_pos == GREATER_THAN)
	{
		if((*bucket_id) == UINT64_MAX)
			return NULL;
		find_pos = GREATER_THAN_EQUALS;
		(*bucket_id) += 1;
	}

	if(find_pos == LESSER_THAN_EQUALS)
	{
		if((*bucket_id) < atrl_p->delegated_local_root_range.first_bucket_id)
			return NULL;
	}
	else if(find_pos == GREATER_THAN_EQUALS)
	{
		if((*bucket_id) > atrl_p->delegated_local_root_range.last_bucket_id)
			return NULL;
	}

	// result to be returned
	const void* result = NULL;

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	if(!initialize_locked_pages_stack(locked_pages_stack_p, atrl_p->max_local_root_level + 1))
		exit(-1);

	// push local root onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(atrl_p->local_root, INVALID_TUPLE_INDEX));

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
		uint32_t child_entry_count_curr_page = get_non_NULL_entry_count_in_array_table_page(&(curr_page->ppage), atrl_p->attd_p);

		if(child_entry_count_curr_page == 0)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page->ppage), atrl_p, transaction_id, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
			continue;
		}

		// actual range of curr_page
		bucket_range curr_page_actual_range = get_bucket_range_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p);

		if(find_pos == LESSER_THAN_EQUALS)
		{
			// inside this if clause
			// curr_page->child_index == INVALID_TUPLE_INDEX -> the page iteration has not been initialized
			// curr_page->child_index == entries_per_page -> the page iteration has been completed
			// else we need to visit the curr_page->child_index child on the curr_page
			if(curr_page->child_index == INVALID_TUPLE_INDEX)
			{
				if((*bucket_id) < curr_page_actual_range.first_bucket_id)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page->ppage), atrl_p, transaction_id, abort_error);
					pop_from_locked_pages_stack(locked_pages_stack_p);
					if(*abort_error)
						goto ABORT_ERROR;
					continue;
				}
				else if((*bucket_id) > curr_page_actual_range.last_bucket_id)
					curr_page->child_index = get_entries_per_page_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p) - 1;
				else
					curr_page->child_index = get_child_index_for_bucket_id_on_array_table_page(&(curr_page->ppage), (*bucket_id), atrl_p->attd_p);
			}

			int pushed = 0;
			while(pushed == 0 && curr_page->child_index != get_entries_per_page_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p))
			{
				uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_page(&(curr_page->ppage), curr_page->child_index, atrl_p->attd_p);
				if(child_page_id != atrl_p->attd_p->pas_p->NULL_PAGE_ID)
				{
					if(is_array_table_leaf_page(&(curr_page->ppage), atrl_p->attd_p))
					{
						(*bucket_id) = get_first_bucket_id_of_array_table_page(&(curr_page->ppage), atrl_p->attd_p) + curr_page->child_index;
						result_page_id = child_page_id;
						break;
					}
					else
					{
						// grab read lock on child page, push it onto the stack and then continue
						persistent_page child_page = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
						if(*abort_error)
							goto ABORT_ERROR;
						push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
						pushed = 1;
					}
				}

				if(curr_page->child_index != 0)
					curr_page->child_index--;
				else
					curr_page->child_index = atrl_p->attd_p->entries_per_page;
			}

			if(result != NULL)
				break;

			if(pushed)
				continue;

			// if nothing is pushed and all children of the page have been seen, then pop the curr_page
			if(curr_page->child_index == get_entries_per_page_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p))
			{
				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page->ppage), atrl_p, transaction_id, abort_error);
				pop_from_locked_pages_stack(locked_pages_stack_p);
				if(*abort_error)
					goto ABORT_ERROR;
				continue;
			}
		}
		else if(find_pos == GREATER_THAN_EQUALS)
		{
			// inside this if clause
			// curr_page->child_index == INVALID_TUPLE_INDEX -> the page iteration has not been initialized
			// curr_page->child_index == attd_p->entries_per_page -> the page iteration has been completed
			// else we need to visit the curr_page->child_index child on the curr_page
			if(curr_page->child_index == INVALID_TUPLE_INDEX)
			{
				if((*bucket_id) > curr_page_actual_range.last_bucket_id)
				{
					release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page->ppage), atrl_p, transaction_id, abort_error);
					pop_from_locked_pages_stack(locked_pages_stack_p);
					if(*abort_error)
						goto ABORT_ERROR;
					continue;
				}
				else if((*bucket_id) < curr_page_actual_range.first_bucket_id)
					curr_page->child_index = 0;
				else
					curr_page->child_index = get_child_index_for_bucket_id_on_array_table_page(&(curr_page->ppage), (*bucket_id), atrl_p->attd_p);
			}

			int pushed = 0;
			while(pushed == 0 && curr_page->child_index != get_entries_per_page_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p))
			{
				uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_page(&(curr_page->ppage), curr_page->child_index, atrl_p->attd_p);
				if(child_page_id != atrl_p->attd_p->pas_p->NULL_PAGE_ID)
				{
					if(is_array_table_leaf_page(&(curr_page->ppage), atrl_p->attd_p))
					{
						(*bucket_id) = get_first_bucket_id_of_array_table_page(&(curr_page->ppage), atrl_p->attd_p) + curr_page->child_index;
						result_page_id = child_page_id;
						break;
					}
					else
					{
						// grab read lock on child page, push it onto the stack and then continue
						persistent_page child_page = acquire_persistent_page_with_lock(atrl_p->pam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
						if(*abort_error)
							goto ABORT_ERROR;
						push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
						pushed = 1;
					}
				}

				curr_page->child_index++;
			}

			if(result != NULL)
				break;

			if(pushed)
				continue;

			// if nothing is pushed and all children of the page have been seen, then pop the curr_page
			if(curr_page->child_index == get_entries_per_page_for_array_table_page(&(curr_page->ppage), atrl_p->attd_p))
			{
				release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(curr_page->ppage), atrl_p, transaction_id, abort_error);
				pop_from_locked_pages_stack(locked_pages_stack_p);
				if(*abort_error)
					goto ABORT_ERROR;
				continue;
			}
		}
	}

	// release all locks from stack bottom first
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(bottom->ppage), atrl_p, transaction_id, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

		if(*abort_error)
			goto ABORT_ERROR;
	}
	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return result;

	ABORT_ERROR:
	// release all locks from stack bottom first
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page_while_preventing_local_root_unlocking(&(bottom->ppage), atrl_p, transaction_id, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}
	deinitialize_locked_pages_stack(locked_pages_stack_p);
	// release lock on local root
	release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
	return NULL;
}

// we do not need to preserve local root in this function, since we are not working with range locker
static void backward_pass_to_free_local_root(uint64_t root_page_id, uint64_t discard_target, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// create a stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the array_table
		persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			return;

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_array_table_page(&root_page, attd_p);

		// create a stack of capacity = levels
		if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
			exit(-1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, 0));
	}

	// forward pass walking down the array_table
	while(1)
	{
		locked_page_info* curr_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// actual range of curr_page
		bucket_range curr_page_actual_range = get_bucket_range_for_array_table_page(&(curr_page->ppage), attd_p);

		// if discard_target is not contained in the actual_range of the curr_page, then break ans start reverse pass
		if(!is_bucket_contained_bucket_range(&curr_page_actual_range, discard_target))
			break;

		// now the page must have its actual_range contain the bucket_id,
		// hence a child_index must exist
		curr_page->child_index = get_child_index_for_bucket_id_on_array_table_page(&(curr_page->ppage), discard_target, attd_p);

		// if this child_index is NULL, then break
		if(is_NULL_at_child_index_in_array_table_page(&(curr_page->ppage), curr_page->child_index, attd_p))
			break;

		// there is no going forward than a leaf
		if(is_array_table_leaf_page(&(curr_page->ppage), attd_p))
			break;

		uint64_t child_page_id = get_child_page_id_at_child_index_in_array_table_page(&(curr_page->ppage), curr_page->child_index, attd_p);

		// OPTIMIZATION
		// below piece of code releases locks on all parents of curr_page, if the curr_page will never become empty, even after NULL-ing the child_index on that page
		// this is an optimization to release early locks, you can remove this part and the functionality will still be identical 
		if(get_non_NULL_entry_count_in_array_table_page(&(curr_page->ppage), attd_p) > 1) // this page will exist even if one of its child is NULLed, so we can release lock on its parents
		{
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the curr_page)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

				if(*abort_error)
					goto ABORT_ERROR;
			}
		}
		// OPTIMIZATION ---x---

		// grab write lock on child page, push it onto the stack and then continue
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// reverse pass
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info curr_page = *get_top_of_locked_pages_stack(locked_pages_stack_p);
		pop_from_locked_pages_stack(locked_pages_stack_p);

		uint32_t child_entry_count_curr_page = get_non_NULL_entry_count_in_array_table_page(&(curr_page.ppage), attd_p);

		if(child_entry_count_curr_page == 0)
		{
			if(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0) // then curr_page can not be the local_root, hence we can free it
			{
				// free the curr_page
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), FREE_PAGE, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
					goto ABORT_ERROR;
				}

				// set NULL_PAGE_ID to its parent page entry
				locked_page_info* parent_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
				set_child_page_id_at_child_index_in_array_table_index_page(&(parent_page->ppage), parent_page->child_index, attd_p->pas_p->NULL_PAGE_ID, attd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}
			else // this page is the root of the array_table, so we can't free it
			{
				// write 0 to the level of the curr_page
				array_table_page_header hdr = get_array_table_page_header(&(curr_page.ppage), attd_p);
				hdr.level = 0;
				set_array_table_page_header(&(curr_page.ppage), &hdr, attd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
					goto ABORT_ERROR;
				}

				// release lock on curr_page
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}
		else if(child_entry_count_curr_page == 1 && !is_array_table_leaf_page(&(curr_page.ppage), attd_p)) // if you can level it down then do it
		{
			// level down curr_page
			level_down_array_table_page(&(curr_page.ppage), attd_p, pam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// push curr_page back into the stack
			push_to_locked_pages_stack(locked_pages_stack_p, &curr_page);
		}
		else
		{
			// release lock on curr_page
			release_lock_on_persistent_page(pam_p, transaction_id, &(curr_page.ppage), NONE_OPTION, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			break;
		}
	}

	// release locks bottom first and exit
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

		if(*abort_error)
			goto ABORT_ERROR;
	}
	deinitialize_locked_pages_stack(locked_pages_stack_p);
	return;

	ABORT_ERROR:;
	// release all locks from stack bottom first
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}
	deinitialize_locked_pages_stack(locked_pages_stack_p);
	return;
}

void delete_array_table_range_locker(array_table_range_locker* atrl_p, const void* transaction_id, int* abort_error)
{
	// we will need to make a second pass from root to discard the local_root
	// if, the range_locker is writable, there was no abort, and local_root is still locked
	// local_root != actual_root
	// and the local_root is empty (i.e. has only NULL_PAGE_IDS)
	int will_need_to_discard_if_empty = (atrl_p->pmm_p != NULL) && ((*abort_error) == 0) &&
										(!is_persistent_page_NULL(&(atrl_p->local_root), atrl_p->pam_p)) &&
										(atrl_p->local_root.page_id != atrl_p->root_page_id) &&
										has_all_NULL_entries_in_array_table_page(&(atrl_p->local_root), atrl_p->attd_p);

	// you need to re enter using the root_page_id and go to this bucket_id to discard the local root
	uint64_t discard_target = atrl_p->attd_p->pas_p->NULL_PAGE_ID;
	if(will_need_to_discard_if_empty)
		discard_target = get_first_bucket_id_of_array_table_page(&(atrl_p->local_root), atrl_p->attd_p);

	if(!is_persistent_page_NULL(&(atrl_p->local_root), atrl_p->pam_p))
	{
		release_lock_on_persistent_page(atrl_p->pam_p, transaction_id, &(atrl_p->local_root), NONE_OPTION, abort_error);
		if(*abort_error)
			will_need_to_discard_if_empty = 0;
	}

	if(will_need_to_discard_if_empty)
		backward_pass_to_free_local_root(atrl_p->root_page_id, discard_target, atrl_p->attd_p, atrl_p->pam_p, atrl_p->pmm_p, transaction_id, abort_error);

	free(atrl_p);

	return;
}