#include<bplus_tree_iterator.h>

#include<locked_pages_stack.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_walk_down.h>
#include<sorted_packed_page_util.h>

#include<persistent_page_functions.h>
#include<tuple.h>

#include<find_position.h>

// fails only on an abort_error
static int adjust_position_for_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const void* transaction_id, int* abort_error)
{
	bpi_p->curr_tuple_index = 0;

	// if the bplus_tree is itself empty then nothing needs to be done any further
	if(is_empty_bplus_tree(bpi_p))
		return 1;

	// find the leaf_tuple_index for the iterator to start with
	{
		const persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		uint32_t tuple_count_on_curr_leaf_page = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		switch(find_pos)
		{
			case MIN :
			{
				bpi_p->curr_tuple_index = 0;
				break;
			}
			case LESSER_THAN :
			{
				bpi_p->curr_tuple_index = find_preceding_in_sorted_packed_page(
										curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, 
										bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned,
										key, bpi_p->bpttd_p->key_def, NULL
									);

				bpi_p->curr_tuple_index = (bpi_p->curr_tuple_index != NO_TUPLE_FOUND) ? bpi_p->curr_tuple_index : 0;
				break;
			}
			case LESSER_THAN_EQUALS :
			{
				bpi_p->curr_tuple_index = find_preceding_equals_in_sorted_packed_page(
										curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, 
										bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned,
										key, bpi_p->bpttd_p->key_def, NULL
									);

				bpi_p->curr_tuple_index = (bpi_p->curr_tuple_index != NO_TUPLE_FOUND) ? bpi_p->curr_tuple_index : 0;
				break;
			}
			case GREATER_THAN_EQUALS :
			{
				bpi_p->curr_tuple_index = find_succeeding_equals_in_sorted_packed_page(
										curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, 
										bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned,
										key, bpi_p->bpttd_p->key_def, NULL
									);

				bpi_p->curr_tuple_index = (bpi_p->curr_tuple_index != NO_TUPLE_FOUND) ? bpi_p->curr_tuple_index : (tuple_count_on_curr_leaf_page - 1);
				break;
			}
			case GREATER_THAN :
			{
				bpi_p->curr_tuple_index = find_succeeding_in_sorted_packed_page(
										curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, 
										bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned,
										key, bpi_p->bpttd_p->key_def, NULL
									);

				bpi_p->curr_tuple_index = (bpi_p->curr_tuple_index != NO_TUPLE_FOUND) ? bpi_p->curr_tuple_index : (tuple_count_on_curr_leaf_page - 1);
				break;
			}
			case MAX :
			{
				bpi_p->curr_tuple_index = tuple_count_on_curr_leaf_page - 1;
				break;
			}
		}
	}

	// iterate next or previous in bplus_tree_iterator, based on the find_pos
	// this is not required for MIN and MAX
	switch(find_pos)
	{
		case MIN :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			// if null we are possibly pointing to some position beyond MIN tuple
			if(tuple_to_skip == NULL)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}
			break;
		}
		case LESSER_THAN :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			if(tuple_to_skip == NULL)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}

			tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, key, bpi_p->bpttd_p->key_def, NULL, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned) >= 0)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case LESSER_THAN_EQUALS :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			if(tuple_to_skip == NULL)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}

			tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, key, bpi_p->bpttd_p->key_def, NULL, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned) > 0)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_EQUALS :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			if(tuple_to_skip == NULL)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}

			tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, key, bpi_p->bpttd_p->key_def, NULL, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned) < 0)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			if(tuple_to_skip == NULL)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}

			tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpi_p->bpttd_p->record_def, bpi_p->bpttd_p->key_element_ids, key, bpi_p->bpttd_p->key_def, NULL, bpi_p->bpttd_p->key_compare_direction, key_element_count_concerned) <= 0)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case MAX :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);

			// adjustment for when the first tuple we point to is invalid
			// if null we are possibly pointing to some position beyond MAX tuple
			if(tuple_to_skip == NULL)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;
			}
			break;
		}
		default :
		{
			break;
		}
	}

	return 1;
}

#include<bplus_tree_walk_down.h>

int initialize_bplus_tree_stacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, locked_pages_stack* lps, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(bpttd_p == NULL || pam_p == NULL)
		return 0;

	// if the lock type is not a read lock, i.e. some pages need to be write locked then pmm_p must not be NULL
	if(lock_type != READ_LOCK && pmm_p == NULL)
		return 0;

	// if the user wants to consider all the key elements then
	// set key_element_count_concerned to bpttd_p->key_element_count
	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpttd_p->key_element_count;

	bpi_p->root_page_id = root_page_id;
	bpi_p->is_stacked = 1;
	bpi_p->lock_type = lock_type;
	bpi_p->lps = (locked_pages_stack){};
	bpi_p->curr_tuple_index = 0;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->pam_p = pam_p;
	bpi_p->pmm_p = pmm_p;

	// if lps is not empty, copy its contents to bpi_p->lps
	if(get_element_count_locked_pages_stack(lps) != 0)
	{
		bpi_p->lps = (*lps); // root page is still locked so it is unchanged, so the level must not have changed, nor do we have to change it's capacity
		(*lps) = (locked_pages_stack){}; // lps is safely made empty
	}
	else
	{
		deinitialize_locked_pages_stack(lps); // destroy the lps that the user provided and make it empty, since no locked are held by it, it can not cause any abort error
		(*lps) = (locked_pages_stack){};

		bpi_p->lps = initialize_locked_pages_stack_for_walk_down(root_page_id, bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	// lps does not hold any memory or resources from this point on
	// if it had any then it has been transferred to the bpi_p->lps

	// walk down for the current value of bpi_p->lps
	walk_down_locking_parent_pages_for_stacked_iterator_using_key(&(bpi_p->lps), key, key_element_count_concerned, find_pos, bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
		return 0;
	}

	// adjust bplus_tree_iterator position
	adjust_position_for_bplus_tree_iterator(bpi_p, key, key_element_count_concerned, find_pos, transaction_id, abort_error);
	if(*abort_error)
	{
		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
		return 0;
	}

	return 1;
}

int initialize_bplus_tree_unstacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(bpttd_p == NULL || pam_p == NULL)
		return 0;

	// a non stacked iterator can not be a READ_LOCK_INTERIOR_WRITE_LOCK_LEAF
	if(lock_type == READ_LOCK_INTERIOR_WRITE_LOCK_LEAF)
		return 0;

	// if the lock type is not a read lock, i.e. some pages need to be write locked then pmm_p must not be NULL
	if(lock_type != READ_LOCK && pmm_p == NULL)
		return 0;

	// if the user wants to consider all the key elements then
	// set key_element_count_concerned to bpttd_p->key_element_count
	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpttd_p->key_element_count;

	bpi_p->root_page_id = root_page_id;
	bpi_p->is_stacked = 0;
	bpi_p->curr_page = get_NULL_persistent_page(pam_p);
	bpi_p->curr_tuple_index = 0;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->pam_p = pam_p;
	bpi_p->pmm_p = pmm_p;

	// walk down for the current value of bpi_p->lps
	bpi_p->curr_page = walk_down_for_iterator_using_key(root_page_id, key, key_element_count_concerned, find_pos, lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// adjust bplus_tree_iterator position
	return adjust_position_for_bplus_tree_iterator(bpi_p, key, key_element_count_concerned, find_pos, transaction_id, abort_error);
}

#include<stdlib.h>

bplus_tree_iterator* get_new_bplus_tree_stacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	bplus_tree_iterator bpi_temp;
	if(!initialize_bplus_tree_stacked_iterator(&bpi_temp, root_page_id, &(locked_pages_stack){}, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error))
		return NULL;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	if(bpi_p == NULL)
		exit(-1);
	(*bpi_p) = bpi_temp;

	return bpi_p;
}

bplus_tree_iterator* get_new_bplus_tree_unstacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	bplus_tree_iterator bpi_temp;
	if(!initialize_bplus_tree_unstacked_iterator(&bpi_temp, root_page_id, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error))
		return NULL;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	if(bpi_p == NULL)
		exit(-1);
	(*bpi_p) = bpi_temp;

	return bpi_p;
}