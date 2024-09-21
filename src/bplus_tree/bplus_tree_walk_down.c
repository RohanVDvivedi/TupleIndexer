#include<bplus_tree_walk_down.h>

#include<page_access_methods.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<storage_capacity_page_util.h>


#include<invalid_tuple_indices.h>

#include<stdlib.h>

static int get_lock_type_for_page_by_page_level(int lock_type, uint32_t level)
{
	// handle standard lock types
	if(lock_type == READ_LOCK || lock_type == WRITE_LOCK)
		return lock_type;

	// else this is READ_LOCK_INTERIOR_WRITE_LOCK_LEAF lock_type
	if(level == 0) // leaf page
		return WRITE_LOCK;
	else
		return READ_LOCK;
}

static persistent_page acquire_root_page_with_lock_optimistically(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// if the lock type requirement is already confirmed
	if(lock_type == READ_LOCK || lock_type == WRITE_LOCK)
		return acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, lock_type, abort_error);

	// Now this is the case for READ_LOCK_INTERIOR_WRITE_LOCK_LEAF

	// first do it by optimistically taking a READ_LOCK, assuming it is an interior page
	persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, READ_LOCK, abort_error);
	if(*abort_error)
		return get_NULL_persistent_page(pam_p);
	uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);
	if(READ_LOCK == get_lock_type_for_page_by_page_level(lock_type, root_page_level)) // if the current level confirms that READ_LOCK is the right kind of lock for the root_page return it
		return root_page;

	// if READ_LOCK is not the required type, then we could in theory upgrade the lock, but that may cause deadlock

	// so release the read lock held
	release_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return get_NULL_persistent_page(pam_p);

	// Now we fall back to re trying the lock with a WRITE_LOCK and downgrade if necessary
	root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return get_NULL_persistent_page(pam_p);
	root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);
	if(WRITE_LOCK == get_lock_type_for_page_by_page_level(lock_type, root_page_level)) // if the current level confirms that WRITE_LOCK is the right kind of lock for the root_page return it
		return root_page;

	// else we downgrade the lock and return it
	downgrade_to_reader_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
		return get_NULL_persistent_page(pam_p);
	}
	return root_page;
}

locked_pages_stack initialize_locked_pages_stack_for_walk_down(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	// get lock on the root page of the bplus_tree
	persistent_page root_page = acquire_root_page_with_lock_optimistically(root_page_id, lock_type, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		return ((locked_pages_stack){});

	// get level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

	// create a stack of capacity = levels
	if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
		exit(-1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, INVALID_TUPLE_INDEX));

	return *locked_pages_stack_p;
}

int walk_down_locking_parent_pages_for_split_insert_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no split requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_record(&(curr_locked_page->ppage), record, bpttd_p->key_element_count, bpttd_p);

		// if you reach here, then curr_locked_page is not a leaf page
		// if curr_locked_page will not require a split, then release locks on all the parent pages of curr_locked_page
		if(!may_require_split_for_insert_for_bplus_tree(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, bpttd_p->index_def))
		{
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the curr_locked_page)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

				if(*abort_error)
					goto ABORT_ERROR;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);

		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// if we reach here, then the top page in the locked_pages_stack_p is likely the leaf page

	// get leaf from locked_pages_stack
	locked_page_info* leaf_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

	// if the leaf_locked_page must not split on insertion of record, then release all locks on all the parent pages
	if(!must_split_for_insert_bplus_tree_leaf_page(&(leaf_locked_page->ppage), record, bpttd_p))
	{
		while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the leaf_locked_page)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	return 1;

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	return 0;
}

int walk_down_locking_parent_pages_for_merge_using_key(locked_pages_stack* locked_pages_stack_p, const void* key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_key(&(curr_locked_page->ppage), key, bpttd_p->key_element_count, bpttd_p);

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(!may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, bpttd_p->index_def, curr_locked_page->child_index))
		{
			// release locks on all the pages in stack except for the the curr_locked_page
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

				if(*abort_error)
					goto ABORT_ERROR;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);

		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// if we reach here, then the top page in the locked_pages_stack_p is likely the leaf page

	// here, we can still perform a find to locate the leaf record that we might be delete (given the key) and check if leaf page then will require merging,
	// but I suspect that is just an overkill, instead let the caller to just perform the delete and run the merge_and_unlock_pages_up

	return 1;

	ABORT_ERROR :;
	// on an abort_error release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	return 0;
}

int walk_down_locking_parent_pages_for_merge_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_record(&(curr_locked_page->ppage), record, bpttd_p->key_element_count, bpttd_p);

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(!may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, bpttd_p->index_def, curr_locked_page->child_index))
		{
			// release locks on all the pages in stack except for the the curr_locked_page
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

				if(*abort_error)
					goto ABORT_ERROR;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);

		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// if we reach here, then the top page in the locked_pages_stack_p is likely the leaf page

	// here, we can still perform a find to locate the leaf record that we might be delete (given the key) and check if leaf page then will require merging,
	// but I suspect that is just an overkill, instead let the caller to just perform the delete and run the merge_and_unlock_pages_up

	return 1;

	ABORT_ERROR :;
	// on an abort_error release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	return 0;
}

int walk_down_locking_parent_pages_for_update_using_record(locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t* release_for_split, uint32_t* release_for_merge, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// initialize relase_for_* to zeros
	(*release_for_merge) = 0;
	(*release_for_split) = 0;

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no split requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_record(&(curr_locked_page->ppage), record, bpttd_p->key_element_count, bpttd_p);

		// if you reach here, then curr_locked_page is not a leaf page
		// if curr_locked_page will not require a split, then we can release locks on all the parent pages of curr_locked_page
		if(!may_require_split_for_insert_for_bplus_tree(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, bpttd_p->index_def))
		{
			// (do not release lock on the curr_locked_page)
			(*release_for_split) = get_element_count_locked_pages_stack(locked_pages_stack_p) - 1;
		}

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(!may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
		{
			// we can release locks on all the pages in stack except for the the curr_locked_page
			(*release_for_merge) = get_element_count_locked_pages_stack(locked_pages_stack_p) - 1;
		}

		// release minimum of the two number of locks min((*release_for_split), (*release_for_merge))
		uint32_t max_locks_we_can_release = min((*release_for_split), (*release_for_merge));
		while(max_locks_we_can_release > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto ABORT_ERROR;

			(*release_for_split)--;
			(*release_for_merge)--;
			max_locks_we_can_release--;
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);

		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// if we reach here, then the top page in the locked_pages_stack_p is likely the leaf page

	// but since record may not be the actual record that gets inserted, deleted or updated, we can no longer infer anything about what to do further

	return 1;

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// initialize relase_for_* to zeros, since we are aborting
	(*release_for_merge) = 0;
	(*release_for_split) = 0;

	return 0;
}

persistent_page walk_down_for_iterator_using_key(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position f_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// only READ_LOCK and WRITE_LOCK values for lock_type make sense for this function
	if(lock_type != READ_LOCK)
		lock_type = WRITE_LOCK;

	persistent_page curr_page = get_NULL_persistent_page(pam_p);

	{
		// get lock on the root page of the bplus_tree in mode (referred by lock_type), this is because root_page can be a leaf page
		persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, lock_type, abort_error);
		if(*abort_error)
			return get_NULL_persistent_page(pam_p);

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		if(root_page_level == 0) // if root is the leaf page, then return it
			return root_page;
		else
		{
			if(lock_type == WRITE_LOCK) // downgrade lock on the root page, if we had to WRITE_LOCK it
			{
				downgrade_to_reader_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
					return get_NULL_persistent_page(pam_p);
				}
			}
		}

		// for reaching the leaf make root page as the curr_page
		curr_page = root_page;
	}

	// perform a downward pass until you reach the leaf
	while(1)
	{
		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&curr_page, bpttd_p))
			break;

		// pre cache level of the curr_locked_page
		uint32_t curr_page_level = get_level_of_bplus_tree_page(&curr_page, bpttd_p);

		uint32_t child_index = 0;

		// figure out which child page to go to next, based on f_type, key and key_element_count_concerned
		switch(f_pos)
		{
			case MIN :
			{
				child_index = -1;
				break;
			}
			case LESSER_THAN_EQUALS :
			case GREATER_THAN :
			{
				child_index = find_child_index_for_key(&curr_page, key, key_element_count_concerned, bpttd_p);
				break;
			}
			case LESSER_THAN :
			case GREATER_THAN_EQUALS :
			{
				child_index = find_child_index_for_key_s_predecessor(&curr_page, key, key_element_count_concerned, bpttd_p);
				break;
			}
			case MAX :
			{
				child_index = get_tuple_count_on_persistent_page(&curr_page, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)) - 1;
				break;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		// only the leaf_page (if the parent's level == 1) is locked with lock_type, all other pages (the parent_pages) are locked in READ_LOCK mode
		uint32_t child_page_level = (curr_page_level - 1);
		uint64_t child_page_id = get_child_page_id_by_child_index(&curr_page, child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, ((child_page_level == 0) ? lock_type : READ_LOCK), abort_error);

		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
			return get_NULL_persistent_page(pam_p);
		}

		release_lock_on_persistent_page(pam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &child_page, NONE_OPTION, abort_error);
			return get_NULL_persistent_page(pam_p);
		}

		curr_page = child_page;
	}

	return curr_page;
}

int walk_down_locking_parent_pages_for_stacked_iterator_using_key(locked_pages_stack* locked_pages_stack_p, const void* key, uint32_t key_element_count_concerned, find_position f_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// perform a downward pass until you reach the leaf locking all the pages
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// pre cache level of the curr_locked_page
		uint32_t curr_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpttd_p);

		// figure out which child page to go to next, based on f_type, key and key_element_count_concerned
		switch(f_pos)
		{
			case MIN :
			{
				curr_locked_page->child_index = -1;
				break;
			}
			case LESSER_THAN_EQUALS :
			case GREATER_THAN :
			{
				curr_locked_page->child_index = find_child_index_for_key(&(curr_locked_page->ppage), key, key_element_count_concerned, bpttd_p);
				break;
			}
			case LESSER_THAN :
			case GREATER_THAN_EQUALS :
			{
				curr_locked_page->child_index = find_child_index_for_key_s_predecessor(&(curr_locked_page->ppage), key, key_element_count_concerned, bpttd_p);
				break;
			}
			case MAX :
			{
				curr_locked_page->child_index = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)) - 1;
				break;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint32_t child_page_level = (curr_page_level - 1);
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, get_lock_type_for_page_by_page_level(lock_type, child_page_level), abort_error);

		// exit for abort_error
		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX));
	}

	// on success return 1
	return 1;

	ABORT_ERROR :;
	// on an abort_error, release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}
	return 0;
}

int walk_down_next_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// pop the top of the stack, which will happen to be leaf_page, exposing us to the parent pages underneath
	{
		locked_page_info* top = get_top_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
		pop_from_locked_pages_stack(locked_pages_stack_p);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// loop over stack to reach the next
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		uint32_t curr_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpttd_p);

		// for an interior page,
		// if it's child_index + 1 is out of bounds then release lock on it and pop it
		// else lock the child_page on it's child_index + 1 entry and push this child_page onto the lps stack
		
		// get tuple_count of the page
		uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

		// increment its child_index
		curr_locked_page->child_index++;

		if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
		{
			// then lock the page at child_index, and push it onto the stack
			uint32_t child_page_level = (curr_page_level - 1);
			uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
			persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, get_lock_type_for_page_by_page_level(lock_type, child_page_level), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// push a locked_page_info for child_page pointing to its (first_child_index - 1)
			locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX);
			if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpttd_p)) // when not leaf
				child_locked_page_info.child_index = ALL_LEAST_KEYS_CHILD_INDEX-1; // this is so that when this becomes initially the top for this loop, it will get incremented to its ALL_LEAST_KEYS_CHILD_INDEX
			push_to_locked_pages_stack(locked_pages_stack_p, &child_locked_page_info);
		}
		else // pop release lock on the curr_locked_page, and pop it
		{
			// pop it from the stack and unlock it
			release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	if(get_element_count_locked_pages_stack(locked_pages_stack_p) == 0) // this implies end of scan
		return 0;

	return 1;

	ABORT_ERROR:;
	// on an abort_error, release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}
	return 0;
}

int walk_down_prev_locking_parent_pages_for_stacked_iterator(locked_pages_stack* locked_pages_stack_p, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// pop the top of the stack, which will happen to be curr_leaf_page, exposing us to the parent pages underneath
	{
		locked_page_info* top = get_top_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
		pop_from_locked_pages_stack(locked_pages_stack_p);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// loop over stack to reach the prev
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		uint32_t curr_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpttd_p);

		// for an interior page,
		// if it's child_index - 1 is out of bounds then release lock on it and pop it
		// else lock the child_page on it's child_index - 1 entry and push this child_page onto the lps stack

		// get tuple_count of the page
		uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

		// decrement its child_index
		curr_locked_page->child_index--;

		if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
		{
			// then lock the page at child_index, and push it onto the stack
			uint32_t child_page_level = (curr_page_level - 1);
			uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
			persistent_page child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, child_page_id, get_lock_type_for_page_by_page_level(lock_type, child_page_level), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// push a locked_page_info for child_page, making its last child + 1,
			locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX);
			if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpttd_p))
				child_locked_page_info.child_index = get_tuple_count_on_persistent_page(&(child_locked_page_info.ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)); // this is so that when this becomes initially the top for this loop, it will get decremented to its last_child_index, which is at (tuple_count-1)
			push_to_locked_pages_stack(locked_pages_stack_p, &child_locked_page_info);
		}
		else // pop release lock on the curr_locked_page, and pop it
		{
			// pop it from the stack and unlock it
			release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	if(get_element_count_locked_pages_stack(locked_pages_stack_p) == 0) // this implies end of scan
		return 0;

	return 1;

	ABORT_ERROR:;
	// on an abort_error, release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}
	return 0;
}

int can_walk_down_next_locking_parent_pages_for_stacked_iterator(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p)
{
	// iterate from the bottom of the stack
	for(uint32_t i = 0; i < get_element_count_locked_pages_stack(locked_pages_stack_p); i++)
	{
		locked_page_info* ppage_to_check = get_from_bottom_of_locked_pages_stack(locked_pages_stack_p, i);

		// leaf pages are not to be considered for this
		// this must surely be the last iteration of the loop, as top most page must be a leaf page
		if(is_bplus_tree_leaf_page(&(ppage_to_check->ppage), bpttd_p))
			break;

		// if there is atleast 1 interior page who is not pointing to its last child, then the walk_down_next will succeed
		if(ppage_to_check->child_index != (get_tuple_count_on_persistent_page(&(ppage_to_check->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)) - 1))
			return 1;
	}

	return 0;
}

int can_walk_down_prev_locking_parent_pages_for_stacked_iterator(const locked_pages_stack* locked_pages_stack_p, const bplus_tree_tuple_defs* bpttd_p)
{
	// iterate from the bottom of the stack
	for(uint32_t i = 0; i < get_element_count_locked_pages_stack(locked_pages_stack_p); i++)
	{
		locked_page_info* ppage_to_check = get_from_bottom_of_locked_pages_stack(locked_pages_stack_p, i);

		// leaf pages are not to be considered for this
		// this must surely be the last iteration of the loop, as top most page must be a leaf page
		if(is_bplus_tree_leaf_page(&(ppage_to_check->ppage), bpttd_p))
			break;

		// if there is atleast 1 interior page who is not pointing to its first child, then the walk_down_prev will succeed
		if(ppage_to_check->child_index != ALL_LEAST_KEYS_CHILD_INDEX)
			return 1;
	}

	return 0;
}

void release_all_locks_and_deinitialize_stack_reenterable(locked_pages_stack* locked_pages_stack_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// release all memory it occupies and leave the stack so that this function is re-enterable
	deinitialize_locked_pages_stack(locked_pages_stack_p);
	(*locked_pages_stack_p) = ((locked_pages_stack){});
}