#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<bplus_tree_walk_down.h>
#include<sorted_packed_page_util.h>

#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<tuple.h>

#include<stdlib.h>

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int is_stacked, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// if the user wants to consider all the key elements then
	// set key_element_count_concerned to bpttd_p->key_element_count
	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpttd_p->key_element_count;

	// a non stacked iterator can not be a READ_LOCK_INTERIOR_WRITE_LOCK_LEAF
	if(is_stacked == 0 && lock_type == READ_LOCK_INTERIOR_WRITE_LOCK_LEAF)
		return NULL;

	// if the lock type is not a read lock, i.e. some pages need to be write locked then pmm_p must not be NULL
	if(lock_type != READ_LOCK && pmm_p == NULL)
		return NULL;

	// initialize both lock holders for is_stacked = 0 and = 1
	locked_pages_stack lps = (locked_pages_stack){};
	persistent_page curr_page = get_NULL_persistent_page(pam_p);

	persistent_page* leaf_page = NULL;

	if(is_stacked)
	{
		lps = initialize_locked_pages_stack_for_walk_down(root_page_id, lock_type, bpttd_p, pam_p, transaction_id, abort_error);
		if(*abort_error)
			return NULL;

		walk_down_locking_parent_pages_for_stacked_iterator_using_key(&lps, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_all_locks_and_deinitialize_stack_reenterable(&lps, pam_p, transaction_id, abort_error);
			return NULL;
		}

		leaf_page = &(get_top_of_locked_pages_stack(&lps)->ppage);
	}
	else
	{
		curr_page = walk_down_for_iterator_using_key(root_page_id, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, transaction_id, abort_error);
		if(*abort_error)
			return NULL;

		leaf_page = &curr_page;
	}

	// find the leaf_tuple_index for the iterator to start with
	uint32_t leaf_tuple_index = 0;

	switch(find_pos)
	{
		case MIN :
		{
			leaf_tuple_index = 0;
			break;
		}
		case LESSER_THAN :
		{
			leaf_tuple_index = find_preceding_in_sorted_packed_page(
									leaf_page, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : 0;
			break;
		}
		case LESSER_THAN_EQUALS :
		{
			leaf_tuple_index = find_preceding_equals_in_sorted_packed_page(
									leaf_page, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : 0;
			break;
		}
		case GREATER_THAN_EQUALS :
		{
			leaf_tuple_index = find_succeeding_equals_in_sorted_packed_page(
									leaf_page, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case GREATER_THAN :
		{
			leaf_tuple_index = find_succeeding_in_sorted_packed_page(
									leaf_page, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case MAX :
		{
			leaf_tuple_index = LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
	}

	bplus_tree_iterator* bpi_p = NULL;

	if(is_stacked)
	{
		bpi_p = get_new_bplus_tree_stacked_iterator(lps, leaf_tuple_index, lock_type, bpttd_p, pam_p, pmm_p);

		// release of resources if we would not create an interator
		if(bpi_p == NULL)
		{
			release_all_locks_and_deinitialize_stack_reenterable(&lps, pam_p, transaction_id, abort_error);
			return NULL;
		}
	}
	else
	{
		bpi_p = get_new_bplus_tree_iterator(curr_page, leaf_tuple_index, bpttd_p, pam_p, pmm_p);

		// release of resources if we would not create an interator
		if(bpi_p == NULL)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
			return NULL;
		}
	}

	// if the bplus_tree is itself empty then nothing needs to be done any further
	if(is_empty_bplus_tree(bpi_p))
		return bpi_p;

	// from this point on lps/curr_page becomes the ownership of the bplus_tree_iterator, it and the leaf_page must not be accessed any further

	// iterate next or previous in bplus_tree_iterator, based on the find_pos
	// this is not required for MIN and MAX
	// if the initial leaf page is empty, then a single next or previous is necessary to be called, based on where you want to move
	switch(find_pos)
	{
		case LESSER_THAN :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) >= 0)
			{
				if(!prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error))
					break;
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case LESSER_THAN_EQUALS :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) > 0)
			{
				if(!prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error))
					break;
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_EQUALS :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) < 0)
			{
				if(!next_bplus_tree_iterator(bpi_p, transaction_id, abort_error))
					break;
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) <= 0)
			{
				if(!next_bplus_tree_iterator(bpi_p, transaction_id, abort_error))
					break;
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		default :
		{
			break;
		}
	}

	return bpi_p;
}