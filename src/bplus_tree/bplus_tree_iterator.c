#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_walk_down.h>

#include<stdlib.h>

persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p)
{
	if(bpi_p->is_stacked)
	{
		locked_page_info* curr_leaf_page = get_top_of_locked_pages_stack(&(bpi_p->lps));
		if(curr_leaf_page == NULL)
			return NULL;
		return &(curr_leaf_page->ppage);
	}
	else
	{
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			return NULL;
		return &(bpi_p->curr_page);
	}
}

// makes the iterator point to next page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_next_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked == 0) // iterate forward using the next_page pointer on the leaf
	{
		// get the next_page_id
		uint64_t next_page_id = get_next_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		// attempt to lock the next_leaf_page
		persistent_page next_leaf_page = get_NULL_persistent_page(bpi_p->pam_p);
		if(next_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			next_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, next_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			// on an abort error release lock on next_leaf_page if it is not NULL
			if(!is_persistent_page_NULL(&next_leaf_page, bpi_p->pam_p))
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &next_leaf_page, NONE_OPTION, abort_error);
			return 0;
		}

		// update the curr_page
		bpi_p->curr_page = next_leaf_page;

		// goto_next was a success if next_leaf_page is not null
		return !is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p);
	}
	else // iterate forward using the pointers on the parent pages that are stacked
		return walk_down_next_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
}

// returns if the above function will succeed
static int can_goto_next_leaf_page(const bplus_tree_iterator* bpi_p)
{
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	if(bpi_p->is_stacked == 0)
	{
		persistent_page* curr_leaf_page = get_curr_leaf_page((bplus_tree_iterator*)bpi_p);
		return has_next_leaf_page(curr_leaf_page, bpi_p->bpttd_p);
	}
	else
		return can_walk_down_next_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->bpttd_p);
}

// makes the iterator point to prev page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_prev_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked == 0) // iterate forward using the prev_page pointer on the leaf
	{
		// get the prev_page_id
		uint64_t prev_page_id = get_prev_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		// attempt to lock the prev_leaf_page
		persistent_page prev_leaf_page = get_NULL_persistent_page(bpi_p->pam_p);
		if(prev_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			prev_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, prev_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			// on an abort error release lock on next_leaf_page if it is not NULL
			if(!is_persistent_page_NULL(&prev_leaf_page, bpi_p->pam_p))
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &prev_leaf_page, NONE_OPTION, abort_error);
			return 0;
		}

		// update the curr_page
		bpi_p->curr_page = prev_leaf_page;

		// goto_next was a success if next_leaf_page is not null
		return !is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p);
	}
	else // iterate forward using the pointers on the parent pages that are stacked
		return walk_down_prev_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
}

// returns if the above function will succeed
static int can_goto_prev_leaf_page(const bplus_tree_iterator* bpi_p)
{
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	if(bpi_p->is_stacked == 0)
	{
		persistent_page* curr_leaf_page = get_curr_leaf_page((bplus_tree_iterator*)bpi_p);
		return has_prev_leaf_page(curr_leaf_page, bpi_p->bpttd_p);
	}
	else
		return can_walk_down_prev_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->bpttd_p);
}

bplus_tree_iterator* clone_bplus_tree_iterator(const bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(is_writable_bplus_tree_iterator(bpi_p))
		return NULL;

	bplus_tree_iterator* clone_p = malloc(sizeof(bplus_tree_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->is_stacked = bpi_p->is_stacked;
	clone_p->curr_tuple_index = bpi_p->curr_tuple_index;
	clone_p->bpttd_p = bpi_p->bpttd_p;
	clone_p->pam_p = bpi_p->pam_p;
	clone_p->pmm_p = bpi_p->pmm_p;

	if(bpi_p->is_stacked)
	{
		clone_p->lock_type = bpi_p->lock_type;
		if(!initialize_locked_pages_stack(&(clone_p->lps), get_capacity_locked_pages_stack(&(bpi_p->lps))))
			exit(-1);

		// clone all the locks of bpi_p->lps in bottom first order and insert them to clone_p->lps
		for(uint32_t i = 0; i < get_element_count_locked_pages_stack(&(bpi_p->lps)); i++)
		{
			locked_page_info* locked_page_info_to_clone = get_from_bottom_of_locked_pages_stack(&(bpi_p->lps), i);
			persistent_page cloned_page = acquire_persistent_page_with_lock(clone_p->pam_p, transaction_id, locked_page_info_to_clone->ppage.page_id, READ_LOCK, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(clone_p->lps), &INIT_LOCKED_PAGE_INFO(cloned_page, locked_page_info_to_clone->child_index));
		}
	}
	else
	{
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			clone_p->curr_page = get_NULL_persistent_page(bpi_p->pam_p);
		else
		{
			clone_p->curr_page = acquire_persistent_page_with_lock(clone_p->pam_p, transaction_id, bpi_p->curr_page.page_id, READ_LOCK, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	return clone_p;

	ABORT_ERROR:
	if(bpi_p->is_stacked)
		release_all_locks_and_deinitialize_stack_reenterable(&(clone_p->lps), clone_p->pam_p, transaction_id, abort_error);
	free(clone_p);
	return NULL;
}

int is_writable_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	return bpi_p->pmm_p != NULL;
}

int is_stacked_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	return bpi_p->is_stacked;
}

int is_empty_bplus_tree(const bplus_tree_iterator* bpi_p)
{
	persistent_page* curr_leaf_page = get_curr_leaf_page((bplus_tree_iterator*)bpi_p);
	return (curr_leaf_page == NULL) || (0 == get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)));
}

int is_beyond_min_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	// if the current tuple index is -1, and there is no prev leaf page
	return (bpi_p->curr_tuple_index == -1) && (!can_goto_prev_leaf_page(bpi_p));
}

int is_beyond_max_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	// if the current tuple index is tuple_count, and there is no next leaf page
	persistent_page* curr_leaf_page = get_curr_leaf_page((bplus_tree_iterator*)bpi_p);
	return (bpi_p->curr_tuple_index == get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def))) && (!can_goto_next_leaf_page(bpi_p));
}

int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	// you can never go next on an empty bplus_tree
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	// you can not go next if you are beyond max_tuple
	if(is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
		return 0;

	bpi_p->curr_tuple_index++;

	{
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if the next tuple we point to will be on this page, return success
		if(bpi_p->curr_tuple_index < get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
			return 1;
	}

	// else we keep visiting the next pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// if there is no where else to go, then we are beyong max_tuple
		if(!can_goto_next_leaf_page(bpi_p))
			break;

		// iterate to next leaf page
		goto_next_leaf_page(bpi_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;

		// get curr_leaf_page we are pointing to
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// a valid leaf page has atleast a tuple, this condition is always true for a non-empty bplus_tree
		if(curr_leaf_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = 0;
			break;
		}
	}

	return 1;
}

const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);
	if(curr_leaf_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return NULL;
	return get_nth_tuple_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def), bpi_p->curr_tuple_index);
}

int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	// you can never go prev on an empty bplus_tree
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	// you can not go prev if you are beyond min_tuple
	if(is_beyond_min_tuple_bplus_tree_iterator(bpi_p))
		return 0;

	bpi_p->curr_tuple_index--;

	{
		// if the next tuple we point to will be on this page, return success
		if(bpi_p->curr_tuple_index != -1)
			return 1;
	}

	// else we keep visiting the previous pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// if there is no where else to go, then we are beyong min_tuple
		if(!can_goto_prev_leaf_page(bpi_p))
			break;

		// iterate to prev leaf page
		goto_prev_leaf_page(bpi_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;

		// get curr_leaf_page we are pointing to
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// a valid page has atleast a tuple, this condition is always true for a non-empty bplus_tree
		if(curr_leaf_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = curr_leaf_page_tuple_count - 1;
			break;
		}
	}

	return 1;
}

void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked)
		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
	else
	{
		if(!is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
	}
	free(bpi_p);
}

int narrow_down_range_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* key1, find_position f_pos1, const void* key2, find_position f_pos2, uint32_t key_element_count_concerned, const void* transaction_id, int* abort_error)
{
	// you can never narrow_down iterator on an empty bplus_tree
	if(is_empty_bplus_tree(bpi_p))
		return 0;

	// does not work with unstacked iterator
	if(!bpi_p->is_stacked)
		return 0;

	// does not work with WRITE_LOCK-ed stacked iterator
	if(bpi_p->lock_type == WRITE_LOCK)
		return 0;

	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpi_p->bpttd_p->key_element_count;

	return narrow_down_range_for_stacked_iterator_using_keys(&(bpi_p->lps), key1, f_pos1, key2, f_pos2, key_element_count_concerned, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
}

#include<sorted_packed_page_util.h>
#include<bplus_tree_split_insert_util.h>
#include<bplus_tree_merge_util.h>

int remove_from_linked_page_list_iterator(bplus_tree_iterator* bpi_p, bplus_tree_after_remove_operation aft_op, const void* transaction_id, int* abort_error)
{
	// does not work on unstacked iterator
	if(!bpi_p->is_stacked)
		return 0;

	// works only on stacked write locked iterator
	if(bpi_p->lock_type != WRITE_LOCK)
		return 0;

	// fail if the current tuple is NULL, the iterator is positioned BEYOND ranges or is empty
	const void* curr_tuple = get_tuple_bplus_tree_iterator(bpi_p);
	if(curr_tuple == NULL)
		return 0;

	// beyond this point it is either success or an abort_error

	void* curr_key = NULL;

	if(aft_op == PREPARE_FOR_DELETE_ITERATOR_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION)
	{
		// count the number of pages that can be unlocked safely and unlock them
		uint32_t safely_unlockable = count_unlockable_parent_pages_for_merge(&(bpi_p->lps), bpi_p->bpttd_p);
		while(safely_unlockable > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));

			if(*abort_error)
				goto ABORT_ERROR;

			safely_unlockable--;
		}
	}
	else // we will need to reposition the iterator so grab the key for the curr_tuple
	{
		curr_key = malloc(bpi_p->bpttd_p->max_index_record_size); // key would be no bigger than the max_index_record_size
		if(!extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(bpi_p->bpttd_p, curr_tuple, curr_key))
		{
			free(curr_key);
			return 0;
		}
	}

	// curr_tuple must not be accessed beyond this point

	// delete the tuple at the curr_tuple_index on the current leaf page
	// this delete must succeed as we know that the tuple is already present
	delete_in_sorted_packed_page(
						get_curr_leaf_page(bpi_p), bpi_p->bpttd_p->pas_p->page_size,
						bpi_p->bpttd_p->record_def,
						bpi_p->curr_tuple_index,
						bpi_p->pmm_p,
						transaction_id,
						abort_error
					);
	if(*abort_error)
		goto ABORT_ERROR;

	merge_and_unlock_pages_up(bpi_p->root_page_id, &(bpi_p->lps), bpi_p->bpttd_p, bpi_p->pam_p, bpi_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	if(aft_op == PREPARE_FOR_DELETE_ITERATOR_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION)
	{
		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}
	else
	{
		// transfer ownership of bpi_p to a local variable
		bplus_tree_iterator bpi_temp = (*bpi_p);
		(*bpi_p) = (bplus_tree_iterator){};

		find_position f_pos = ((aft_op == GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION) ? GREATER_THAN : LESSER_THAN);

		// this call also may only fail on an abort error -> which will make the bpi_temp.lps locks released and its memory freed
		initialize_bplus_tree_stacked_iterator(bpi_p, bpi_temp.root_page_id, &(bpi_temp.lps), curr_key, bpi_temp.bpttd_p->key_element_count, f_pos, bpi_temp.lock_type, bpi_temp.bpttd_p, bpi_temp.pam_p, bpi_temp.pmm_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_all_locks_and_deinitialize_stack_reenterable(&(bpi_temp.lps), bpi_p->pam_p, transaction_id, abort_error);
			goto ABORT_ERROR;
		}

		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_temp.lps), bpi_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		free(curr_key);
	}

	return 1;

	// TODO

	ABORT_ERROR:
	if(curr_key != NULL)
		free(curr_key);
	release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
	return 0;
}

int update_at_linked_page_list_iterator(bplus_tree_iterator* bpi_p, const void* tuple, int prepare_for_delete_iterator_on_success, const void* transaction_id, int* abort_error)
{
	// TODO
}

int update_non_key_element_in_place_at_bplus_tree_iterator(bplus_tree_iterator* bpi_p, positional_accessor element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
{
	// cannot update non WRITE_LOCKed bplus_tree_iterator
	if(!is_writable_bplus_tree_iterator(bpi_p))
		return 0;

	// make sure that the element that the user is trying to update in place is not a key for the bplus_tree
	// if you allow so, it could be a disaster
	for(uint32_t i = 0; i < bpi_p->bpttd_p->key_element_count; i++)
	{
		int match = 1;
		for(uint32_t j = 0; j < min(bpi_p->bpttd_p->key_element_ids[i].positions_length, element_index.positions_length); j++)
		{
			if(bpi_p->bpttd_p->key_element_ids[i].positions[j] != element_index.positions[j])
			{
				match = 0;
				break;
			}
		}
		if(match)
			return 0;
	}

	// get current leaf page that the bplus_tree_iterator is pointing to
	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

	// if the iterator is at the end OR is not pointing to a valid tuple, then fail
	if(curr_leaf_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return 0;

	// perform the inplace update, on an abort release all locks
	int updated = set_element_in_tuple_in_place_on_persistent_page(bpi_p->pmm_p, transaction_id, curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, bpi_p->bpttd_p->record_def, bpi_p->curr_tuple_index, element_index, element_value, abort_error);
	if(*abort_error)
	{
		while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
		}
		return 0;
	}

	return updated;
}

void debug_print_lock_stack_for_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	if(bpi_p->is_stacked)
	{
		printf("STACKED : ");
		for(uint32_t i = 0; i < get_element_count_locked_pages_stack(&(bpi_p->lps)); i++)
		{
			locked_page_info* t = get_from_bottom_of_locked_pages_stack(&(bpi_p->lps), i);
			printf("%s(%"PRIu64") -> %"PRIu32", ", (is_persistent_page_write_locked(&(t->ppage)) ? "WRITE_LOCK" : "READ_LOCK"), t->ppage.page_id, t->child_index);
		}
		printf(" -> %"PRIu32"\n", bpi_p->curr_tuple_index);
	}
	else
	{
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			printf("UNSTACKED : \n");
		else
			printf("UNSTACKED : %s(%"PRIu64") -> %"PRIu32"\n", (is_persistent_page_write_locked(&(bpi_p->curr_page)) ? "WRITE_LOCK" : "READ_LOCK"), bpi_p->curr_page.page_id, bpi_p->curr_tuple_index);
	}
}