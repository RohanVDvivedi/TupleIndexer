#include<hash_table_iterator.h>

#include<linked_page_list.h>

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

	// fetch the bucket_count of the hash_table
	// take a range lock on the page table, to get the bucket_count
	hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_EVERYTHING_AND_ABORT;

	// get the current bucket_count of the hash_table
	hti_p->bucket_count = UINT64_MAX;
	find_non_NULL_PAGE_ID_in_page_table(hti_p->ptrl_p, &(hti_p->bucket_count), LESSER_THAN_EQUALS, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_EVERYTHING_AND_ABORT;

	if(hti_p->key != NULL)
	{
		// get the bucket for the key
		hti_p->curr_bucket_id = get_bucket_index_for_key_using_hash_table_tuple_definitions(hti_p->httd_p, hti_p->key, hti_p->bucket_count);

		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
		if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// open a linked_page_list_iterator at bucket_head_page_id
			hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			// close the hti_p->ptrl_p
			delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
			hti_p->ptrl_p = NULL;
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;
		}
	}
	else
	{
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
		delete_page_table_range_locker(clone_p->ptrl_p, transaction_id, abort_error);
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
	if(hti_p->key != NULL && 0 != compare_tuples(record, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, hti_p->key, hti_p->httd_p->key_def, NULL, NULL, hti_p->httd_p->key_element_count))
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
	if(0 != compare_tuples(tuple, hti_p->httd_p->lpltd.record_def, hti_p->httd_p->key_element_ids, hti_p->key, hti_p->httd_p->key_def, NULL, NULL, hti_p->httd_p->key_element_count))
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
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
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

int update_non_key_element_in_place_at_hash_table_iterator(hash_table_iterator* hti_p, uint32_t element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
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
		if(hti_p->httd_p->key_element_ids[i] == element_index)
			return 0;

	// go ahead with the actual update
	// you may not access curr_tuple beyond the below call
	int result = update_element_in_place_at_linked_page_list_iterator(hti_p->lpli_p, element_index, element_value, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return result;

	ABORT_ERROR:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	return 0;
}

void delete_hash_table_iterator(hash_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	// if it is a keyed iterator, then it is our duty to NULL the empty bucket linked_page_list
	if((hti_p->key != NULL) && (!(*abort_error)) && (hti_p->pmm_p != NULL) && (hti_p->lpli_p != NULL) && (hti_p->ptrl_p == NULL) && is_empty_linked_page_list(hti_p->lpli_p))
	{
		// release lock on hti_p->lpli_p
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
		hti_p->lpli_p = NULL;
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		// take a range lock on the page table, to get the bucket_count
		hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		// get the current bucket_count of the hash_table
		uint64_t bucket_count = UINT64_MAX;
		find_non_NULL_PAGE_ID_in_page_table(hti_p->ptrl_p, &bucket_count, LESSER_THAN_EQUALS, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;

		// go to the bucket that key points to
		hti_p->curr_bucket_id = get_bucket_index_for_key_using_hash_table_tuple_definitions(hti_p->httd_p, hti_p->key, bucket_count);

		// 
		uint64_t curr_bucket_head_page_id = get_from_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
		if(curr_bucket_head_page_id != hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// open a linked_page_list_iterator at bucket_head_page_id
			hti_p->lpli_p = get_new_linked_page_list_iterator(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			int is_curr_bucket_empty = is_empty_linked_page_list(hti_p->lpli_p);

			// release lock on hti_p->lpli_p
			delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
			hti_p->lpli_p = NULL;
			if(*abort_error)
				goto DELETE_EVERYTHING_AND_ABORT;

			// if the corresponding linked_page_list exists and is empty, then delete it and set it's entry in page_table as NULL
			if(is_curr_bucket_empty)
			{
				// destroy the linked_page_list
				destroy_linked_page_list(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;

				set_in_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;
			}
		}

		// delete range locker
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
		hti_p->ptrl_p = NULL;
		if(*abort_error)
			goto DELETE_EVERYTHING_AND_ABORT;
	}

	DELETE_EVERYTHING_AND_ABORT:;
	if(hti_p->ptrl_p)
		delete_page_table_range_locker(hti_p->ptrl_p, transaction_id, abort_error);
	if(hti_p->lpli_p)
		delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
	free(hti_p);
}