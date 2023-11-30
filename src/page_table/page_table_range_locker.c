#include<page_table_range_locker.h>

#include<persistent_page_functions.h>
#include<page_table_page_util.h>
#include<page_table_page_header.h>

#include<stdlib.h>

page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, page_table_bucket_range lock_range, int lock_type, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// fail if the lock range is invalid
	if(!is_valid_page_table_bucket_range(&lock_range))
		return NULL;

	// the following 2 must be present
	if(pttd_p == NULL || pam_p == NULL)
		return NULL;

	page_table_range_locker* ptrl_p = malloc(sizeof(page_table_range_locker));
	if(ptrl_p == NULL)
		exit(-1);

	// start initializing the ptrl, making it point to and lock the actual root of the page_table
	ptrl_p->delegated_local_root_range = WHOLE_PAGE_TABLE_BUCKET_RANGE;
	ptrl_p->max_local_root_level = pttd_p->max_page_table_height - 1;
	ptrl_p->local_root = acquire_persistent_page_with_lock(ptrl_p->pam_p, transaction_id, root_page_id, lock_type, abort_error);
	if(*abort_error)
	{
		free(ptrl_p);
		return NULL;
	}
	ptrl_p->root_page_id = root_page_id;
	ptrl_p->pttd_p = pttd_p;
	ptrl_p->pam_p = pam_p;

	// minimize the lock range, from [0,UINT64_MAX] to lock_range
	minimize_lock_range_for_page_table_range_locker(ptrl_p, lock_range, transaction_id, abort_error);
	if(*abort_error)
	{
		free(ptrl_p);
		return NULL;
	}

	return ptrl_p;
}

int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, page_table_bucket_range lock_range, const void* transaction_id, int* abort_error)
{
	// fail, if lock_range is invalid OR if the lock_range is not contained within the delegated range of the local_root
	if(!is_valid_page_table_bucket_range(&lock_range) || !is_contained_page_table_bucket_range(&(ptrl_p->delegated_local_root_range), &lock_range))
		return 0;

	while(1)
	{
		// if the local_root is already a leaf page then quit
		if(is_page_table_leaf_page(&(ptrl_p->local_root), ptrl_p->pttd_p))
			break;

		page_table_bucket_range actual_range = get_bucket_range_for_page_table_page(&(ptrl_p->local_root), ptrl_p->pttd_p);

		// if the actual_range is not contained within the lock_range provided then quit
		if(!is_contained_page_table_bucket_range(&actual_range, &lock_range))
			break;

		uint32_t first_bucket_child_index = get_child_index_for_bucket_id_on_page_table_page(&(ptrl_p->local_root), lock_range.first_bucket_id, ptrl_p->pttd_p);
		uint32_t last_bucket_child_index = get_child_index_for_bucket_id_on_page_table_page(&(ptrl_p->local_root), lock_range.last_bucket_id, ptrl_p->pttd_p);

		// the lock_range must point to a single child on the local_root
		if(first_bucket_child_index != last_bucket_child_index)
			break;

		// this is the child we must go to
		// its delegated_range contains the lock_range
		uint32_t child_index = first_bucket_child_index;

		// get child_page_id to go to
		uint64_t child_page_id = get_child_page_id_at_child_index_in_page_table_page(&(ptrl_p->local_root), child_index, ptrl_p->pttd_p);

		// if this entry does not point to an existing child, then we can not make it the local root
		if(child_page_id == ptrl_p->pttd_p->pas_p->NULL_PAGE_ID)
			break;

		// update the local_root's delegated_range and max_level
		ptrl_p->delegated_local_root_range = get_delegated_bucket_range_for_child_index_on_page_table_page(&(ptrl_p->local_root), child_index, ptrl_p->pttd_p);
		ptrl_p->max_local_root_level = get_level_of_page_table_page(&(ptrl_p->local_root), ptrl_p->pttd_p) - 1;

		// acquire lock on new_local_root, then release lock on old local_root
		persistent_page new_local_root = acquire_persistent_page_with_lock(ptrl_p->pam_p, transaction_id, child_page_id, (is_persistent_page_write_locked(&(ptrl_p->local_root)) ? WRITE_LOCK : READ_LOCK), abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(ptrl_p->pam_p, transaction_id, &(ptrl_p->local_root), NONE_OPTION, abort_error);
			return 0;
		}
		release_lock_on_persistent_page(ptrl_p->pam_p, transaction_id, &(ptrl_p->local_root), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(ptrl_p->pam_p, transaction_id, &new_local_root, NONE_OPTION, abort_error);
			return 0;
		}

		// update the new local_root
		ptrl_p->local_root = new_local_root;
	}

	return 1;
}

page_table_bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p)
{
	return ptrl_p->delegated_local_root_range;
}

int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p)
{
	return is_persistent_page_write_locked(&(ptrl_p->local_root));
}

// release lock on the persistent_page, and ensure that local_root is not unlocked
// only used in get and set functions
static void release_lock_on_persistent_page_while_preventing_local_root_unlocking(persistent_page* ppage, page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error)
{
	// if it is the local_root you are releasing lock on, then you only need to copy it back to local_root of ptrl
	if(ppage->page_id == ptrl_p->local_root.page_id)
	{
		ptrl_p->local_root = (*ppage);
		(*ppage) = get_NULL_persistent_page(ptrl_p->pam_p);
		return;
	}
	release_lock_on_persistent_page(ptrl_p->pam_p, transaction_id, ppage, NONE_OPTION, abort_error);
}

uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, const void* transaction_id, int* abort_error)
{
	// fail if the bucket_id is not contained within the delegated range of the local_root
	if(!is_bucket_contained_page_table_bucket_range(&(ptrl_p->delegated_local_root_range), bucket_id))
		return ptrl_p->pttd_p->pas_p->NULL_PAGE_ID;

	persistent_page curr_page = ptrl_p->local_root;
	while(1)
	{
		// if the curr_page has been locked, then its delegated range will and must contain the bucket_id

		// actual range of curr_page
		page_table_bucket_range curr_page_actual_range = get_bucket_range_for_page_table_page(&curr_page, ptrl_p->pttd_p);

		// if bucket_id is not contained in the actual_range of the curr_page, then return NULL_PAGE_ID
		if(!is_bucket_contained_page_table_bucket_range(&curr_page_actual_range, bucket_id))
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, ptrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return ptrl_p->pttd_p->pas_p->NULL_PAGE_ID;
		}

		// get child_page_id to goto next
		uint32_t child_index = get_child_index_for_bucket_id_on_page_table_page(&curr_page, bucket_id, ptrl_p->pttd_p);
		uint64_t child_page_id = get_child_page_id_at_child_index_in_page_table_page(&curr_page, child_index, ptrl_p->pttd_p);

		// if it is a leaf page, then we return the child_page_id
		if(is_page_table_leaf_page(&curr_page, ptrl_p->pttd_p))
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, ptrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return child_page_id;
		}

		// if not leaf and the child_page_id == NULL_PAGE_ID, then there can be no leaf that contains this bucket_id
		if(child_page_id == ptrl_p->pttd_p->pas_p->NULL_PAGE_ID)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, ptrl_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			return ptrl_p->pttd_p->pas_p->NULL_PAGE_ID;
		}

		// we need to READ_LOCK the child_page, then release lock on the curr_page
		persistent_page child_page = acquire_persistent_page_with_lock(ptrl_p->pam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, ptrl_p, transaction_id, abort_error);
			goto ABORT_ERROR;
		}
		release_lock_on_persistent_page_while_preventing_local_root_unlocking(&curr_page, ptrl_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page_while_preventing_local_root_unlocking(&child_page, ptrl_p, transaction_id, abort_error);
			goto ABORT_ERROR;
		}

		// now make child_page as the curr_page and loop over
		curr_page = child_page;
	}

	ABORT_ERROR:;
	// release lock on local root
	release_lock_on_persistent_page(ptrl_p->pam_p, transaction_id, &(ptrl_p->local_root), NONE_OPTION, abort_error);
	return ptrl_p->pttd_p->pas_p->NULL_PAGE_ID;
}

int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// we cannot set if the ptrl is not locked for reading
	if(!is_writable_page_table_range_locker(ptrl_p))
		return 0;

	// fail if the bucket_id is not contained within the delegated range of the local_root
	if(!is_bucket_contained_page_table_bucket_range(&(ptrl_p->delegated_local_root_range), bucket_id))
		return 0;

	// TODO
}

void delete_page_table_range_locker(page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error);