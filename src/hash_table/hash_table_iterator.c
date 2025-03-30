#include<tupleindexer/hash_table/hash_table_iterator.h>

#include<tupleindexer/linked_page_list/linked_page_list.h>

#include<stdlib.h>

hash_table_iterator* get_new_hash_table_iterator(uint64_t root_page_id, bucket_range lock_range, const void* key, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(httd_p == NULL || pam_p == NULL)
		return NULL;

	// if key is absent, then the lock_range should be valid
	if(key == NULL && !is_valid_bucket_range(&lock_range))
		return NULL;

	hash_table_iterator* hti_p = malloc(sizeof(hash_table_iterator));
	if(hti_p == NULL)
		exit(-1);

	hti_p->root_page_id = root_page_id;
	hti_p->key = key;
	hti_p->lock_range = lock_range;
	hti_p->httd_p = httd_p;
	hti_p->pam_p = pam_p;
	hti_p->pmm_p = pmm_p;

	hti_p->ptrl_p = NULL;
	hti_p->lpli_p = NULL;

	// initialize mat_key
	if(hti_p->key == NULL)
		hti_p->mat_key = (materialized_key){};
	else
		hti_p->mat_key = materialize_key_from_tuple(hti_p->key, hti_p->httd_p->key_def, NULL, hti_p->httd_p->key_element_count);

	if(hti_p->key != NULL)
	{
		const page_modification_methods* curr_pmm_p = NULL; // first attempt with a read lock then if necessary attempt with a write lock
		while(1)
		{
			// fetch the bucket_count of the hash_table
			// take a range lock on the page table, to get the bucket_count -> do this with curr_pmm_p
			hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, curr_pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			// get the current bucket_count of the hash_table
			find_non_NULL_PAGE_ID_in_page_table(hti_p->ptrl_p, &(hti_p->bucket_count), MAX, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			// get the bucket for the key
			hti_p->curr_bucket_id = get_bucket_index_for_key_using_hash_table_tuple_definitions(hti_p->httd_p, hti_p->key, hti_p->bucket_count);

			// once the curr_bucket_id is calculated, we do not need to deal with any other buckets, if we were provided with key
			minimize_lock_range_for_page_table_range_locker(hti_p->ptrl_p, (bucket_range){hti_p->curr_bucket_id, hti_p->curr_bucket_id}, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;
			if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
			{
				// open a linked_page_list_iterator at bucket_head_page_id
				// this must be done with the actual provided pmm_p
				hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;

				// close the hti_p->ptrl_p
				delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, we did not modify anything
				hti_p->ptrl_p = NULL;
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;

				break;
			}
			else // now we only need lock on the page table
			{
				if(curr_pmm_p == hti_p->pmm_p) // if the acquired lock type is with correct pmm_p, then break out of the loop
					break;
				else // else release all locks on the page table and reacquire with the correct lock mode
				{
					// close the hti_p->ptrl_p
					delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, we did not modify anything
					hti_p->ptrl_p = NULL;
					if(*abort_error)
						goto DELETE_EVERYTHING_AND_ABORT;

					curr_pmm_p = hti_p->pmm_p; // this next time reacquire with correct locking
				}
			}
		}
	}
	else
	{
		// fetch the bucket_count of the hash_table
		// take a range lock on the page table, to get the bucket_count
		hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		// get the current bucket_count of the hash_table
		find_non_NULL_PAGE_ID_in_page_table(hti_p->ptrl_p, &(hti_p->bucket_count), MAX, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		// limit lock_range to subset [0, bucket_count-1], discarding the buckets that come after bucket_count buckets
		if(hti_p->lock_range.first_bucket_id >= hti_p->bucket_count)
			goto DELETE_EVERYTHING_AND_ABORT;
		hti_p->lock_range.last_bucket_id = min(hti_p->lock_range.last_bucket_id, hti_p->bucket_count - 1);

		// make curr_bucket_id point to the first bucket in the range
		hti_p->curr_bucket_id = hti_p->lock_range.first_bucket_id;

		// minimize the lock range to the preferred one
		minimize_lock_range_for_page_table_range_locker(hti_p->ptrl_p, hti_p->lock_range, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
		if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// open a linked_page_list_iterator at bucket_head_page_id
			hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;
		}
	}

	return hti_p;

	DELETE_EVERYTHING_AND_ABORT:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	free(hti_p);
	return NULL;
}

hash_table_iterator* clone_hash_table_iterator(const hash_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	if(is_writable_hash_table_iterator(hti_p))
		return NULL;

	hash_table_iterator* clone_p = malloc(sizeof(hash_table_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->root_page_id = hti_p->root_page_id;
	clone_p->key = hti_p->key;
	clone_p->mat_key = hti_p->mat_key; // key remains the same, so mat_key can also be copied, possibly pointing to contents in the same key
	clone_p->lock_range = hti_p->lock_range;
	clone_p->curr_bucket_id = hti_p->curr_bucket_id;
	clone_p->ptrl_p = NULL;
	clone_p->lpli_p = NULL;
	clone_p->httd_p = hti_p->httd_p;
	clone_p->pam_p = hti_p->pam_p;
	clone_p->pmm_p = hti_p->pmm_p;
	clone_p->bucket_count = hti_p->bucket_count;

	if(hti_p->ptrl_p != NULL)
	{
		clone_p->ptrl_p = clone_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
	}

	if(hti_p->lpli_p != NULL)
	{
		clone_p->lpli_p = clone_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
	}

	return clone_p;

	DELETE_EVERYTHING_AND_ABORT:;
	if(clone_p->ptrl_p)
		delete_page_table_range_locker(clone_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no abort required here, as we did not make any update
	if(clone_p->lpli_p)
		delete_linked_page_list_iterator(clone_p->lpli_p, transaction_id, abort_error);
	free(clone_p);
	return NULL;
}

uint64_t get_bucket_count_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return hti_p->bucket_count;
}

int is_writable_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return hti_p->pmm_p != NULL;
}

uint64_t get_curr_bucket_index_for_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return hti_p->curr_bucket_id;
}

int is_curr_bucket_empty_for_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return (hti_p->lpli_p == NULL) || is_empty_linked_page_list(hti_p->lpli_p);
}

int is_curr_bucket_full_for_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return (hti_p->lpli_p != NULL) && (MANY_NODE_LINKED_PAGE_LIST == get_state_for_linked_page_list(hti_p->lpli_p));
}

const void* get_tuple_hash_table_iterator(const hash_table_iterator* hti_p)
{
	// actual tuples exist in buckets stored as linked_page_list, if for some reason this iterator is not open then we can not return a valid tuple
	if(hti_p->lpli_p == NULL)
		return NULL;

	// fetch the record
	const void* record = get_tuple_linked_page_list_iterator(hti_p->lpli_p);

	// this happens if the bucket is empty
	if(record == NULL)
		return NULL;

	// if the key is provided, and the key does not match for this record then wew return NULL
	if(hti_p->key != NULL && 0 != compare_tuple_with_user_value(record, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, hti_p->mat_key.keys, hti_p->mat_key.key_dtis, NULL, hti_p->httd_p->key_element_count))
		return NULL;

	return record;
}

int next_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error)
{
	// check if there is no next tuple, if so we would need to jump bucket
	if(hti_p->lpli_p == NULL || is_empty_linked_page_list(hti_p->lpli_p) || is_at_tail_tuple_linked_page_list_iterator(hti_p->lpli_p))
	{
		// if the key is not NULL, then fail this call
		if(hti_p->key != NULL)
			return 0;

		// if you are not allowed to jump buckets, then fail this call
		if(!can_jump_bucket)
			return 0;

		// if you are already at the last_bucket_id, then fail this call
		if(hti_p->curr_bucket_id == hti_p->lock_range.last_bucket_id)
			return 0;

		// free the existing iterator, over the linked_page_list bucket
		if(hti_p->lpli_p != NULL)
		{
			delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
			hti_p->lpli_p = NULL;
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// increment the curr_bucket_id to point to the next bucket_id
		hti_p->curr_bucket_id++;

		// fecth the head_page_id for the new curr_bucket, and open a bucket iterator over it
		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// open a linked_page_list_iterator at bucket_head_page_id
			hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		return 1;
	}

	// goto next tuple in the same bucket
	int result = next_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are anyway aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int prev_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error)
{
	// check if there is no prev tuple, if so we would need to jump bucket
	if(hti_p->lpli_p == NULL || is_empty_linked_page_list(hti_p->lpli_p) || is_at_head_tuple_linked_page_list_iterator(hti_p->lpli_p))
	{
		// if the key is not NULL, then fail this call
		if(hti_p->key != NULL)
			return 0;

		// if you are not allowed to jump buckets, then fail this call
		if(!can_jump_bucket)
			return 0;

		// if you are already at the first_bucket_id, then fail this call
		if(hti_p->curr_bucket_id == hti_p->lock_range.first_bucket_id)
			return 0;

		// free the existing iterator, over the linked_page_list bucket
		if(hti_p->lpli_p != NULL)
		{
			delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
			hti_p->lpli_p = NULL;
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// decrement the curr_bucket_id to point to the next bucket_id
		hti_p->curr_bucket_id--;

		// fecth the head_page_id for the new curr_bucket, and open a bucket iterator over it
		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// open a linked_page_list_iterator at bucket_head_page_id
			hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		return 1;
	}

	// goto prev tuple in the same bucket
	int result = prev_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are anyway aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int insert_in_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error)
{
	// iterator must be writable
	if(!is_writable_hash_table_iterator(hti_p))
		return 0;

	// if it is not a keyed iterator, then fail
	if(hti_p->key == NULL)
		return 0;

	// if the key doesn't match the tuple, then fail
	if(0 != compare_tuple_with_user_value(tuple, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, hti_p->mat_key.keys, hti_p->mat_key.key_dtis, NULL, hti_p->httd_p->key_element_count))
		return 0;

	// if a linked_page_list at the curr_bucket_id does not exist then create one
	// and make the iterator point from ptrl_p to lpli_p, for the given key
	if(hti_p->lpli_p == NULL)
	{
		uint64_t curr_bucket_head_page_id = get_new_linked_page_list(&(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// set the new bucket head in the pre-calculated curr_bucket_id
		set_in_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, curr_bucket_head_page_id, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// open a bucket iterator for the new linked_page_list bucket
		hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// now you may release locks on the range_locker iterator ptrl_p
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error);  // no vaccum required here, since we only performed a set with a non NULL value
		hti_p->ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// perform a simple insert to this open bucket
	int result = insert_at_linked_page_list_iterator(hti_p->lpli_p, tuple, INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are anyway aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int update_at_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error)
{
	// iterator must be writable
	if(!is_writable_hash_table_iterator(hti_p))
		return 0;

	// fetch the tuple we would be updating
	const void* curr_tuple = get_tuple_hash_table_iterator(hti_p);
	// you can not update a non-existing tuple, so fail if curr_tuple is NULL
	if(curr_tuple == NULL)
		return 0;

	// ensure that the key(curr_tuple) == key(tuple)
	if(0 != compare_tuples(tuple, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, curr_tuple, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, NULL, hti_p->httd_p->key_element_count))
		return 0;

	// go ahead with the actual update
	// you may not access curr_tuple beyond the below call
	int result = update_at_linked_page_list_iterator(hti_p->lpli_p, tuple, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are anyway aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int remove_from_hash_table_iterator(hash_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	// iterator must be writable
	if(!is_writable_hash_table_iterator(hti_p))
		return 0;

	// fetch the tuple we would be removing
	const void* curr_tuple = get_tuple_hash_table_iterator(hti_p);
	// you can not remove a non-existing tuple, so fail if curr_tuple is NULL
	if(curr_tuple == NULL)
		return 0;

	// figure out whether to go next or prev after the removal
	linked_page_list_go_after_operation aft_op = GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION;
	if(is_at_tail_tuple_linked_page_list_iterator(hti_p->lpli_p))
		aft_op = GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION;

	// perform actual removal
	int result = remove_from_linked_page_list_iterator(hti_p->lpli_p, aft_op, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// we are done if the current bucket is not empty
	if(!is_empty_linked_page_list(hti_p->lpli_p))
		return result;

	// if the range for the bucket is not locked
	if(hti_p->ptrl_p == NULL)
		return result;

	// here is where we destroy this empty bucket, and make its pointer in ptrl_p NULL
	// below logic works on the fact the ptrl_p has the lock range that includes curr_bucket_id, and that the page_id corresponding to curr_bucket_id in the ptrl_p is that of the currently open bucket iterator
	{
		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// close the iterator on the bucket
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
		hti_p->lpli_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;

		// now we can destroy the linked_page_list bucket
		destroy_linked_page_list(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// now we can set the bucket_head_page_id corresponding to curr_bucket_id to NULL_PAGE_ID
		set_in_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum required here, since we are anyway aborting
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int update_non_key_element_in_place_at_hash_table_iterator(hash_table_iterator* hti_p, positional_accessor element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
{
	// iterator must be writable
	if(!is_writable_hash_table_iterator(hti_p))
		return 0;

	// fetch the tuple we would be updating
	const void* curr_tuple = get_tuple_hash_table_iterator(hti_p);
	// you can not update a non-existing tuple, so fail if curr_tuple is NULL
	if(curr_tuple == NULL)
		return 0;

	// make sure that the element that the user is trying to update in place is not a key for the hash_table
	// if you allow so, it could be a disaster
	for(uint32_t i = 0; i < hti_p->httd_p->key_element_count; i++)
	{
		int match = 1;
		for(uint32_t j = 0; j < min(hti_p->httd_p->key_element_ids[i].positions_length, element_index.positions_length); j++)
		{
			if(hti_p->httd_p->key_element_ids[i].positions[j] != element_index.positions[j])
			{
				match = 0;
				break;
			}
		}
		if(match)
			return 0;
	}

	// go ahead with the actual update
	// you may not access curr_tuple beyond the below call
	int result = update_element_in_place_at_linked_page_list_iterator(hti_p->lpli_p, element_index, element_value, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, NULL, NULL, transaction_id, abort_error); // no vaccum needed here as the tuple stiull exists and nothing has been logically deleted
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

void delete_hash_table_iterator(hash_table_iterator* hti_p, hash_table_vaccum_params* htvp, const void* transaction_id, int* abort_error)
{
	destroy_materialized_key(&(hti_p->mat_key));

	htvp->page_table_vaccum_needed = 0;
	htvp->hash_table_vaccum_needed = 0;
	htvp->hash_table_vaccum_key = NULL;

	// if it is a keyed iterator, then it is our duty to NULL the empty bucket linked_page_list, as a part of vaccum
	if((hti_p->key != NULL) && (!(*abort_error)) && (hti_p->pmm_p != NULL) && (hti_p->lpli_p != NULL) && (hti_p->ptrl_p == NULL) && is_empty_linked_page_list(hti_p->lpli_p))
	{
		htvp->hash_table_vaccum_needed = 1;
		htvp->hash_table_vaccum_key = hti_p->key;
	}

	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, &(htvp->page_table_vaccum_bucket_id), &(htvp->page_table_vaccum_needed), transaction_id, abort_error);
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	free(hti_p);

	// on abort no vaccum is needed
	if(*abort_error)
	{
		htvp->hash_table_vaccum_needed = 0;
		htvp->page_table_vaccum_needed = 0;
	}
}