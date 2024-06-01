#include<hash_table_iterator.h>

#include<linked_page_list.h>

#include<stdlib.h>

hash_table_iterator* get_new_hash_table_iterator(uint64_t root_page_id, page_table_bucket_range bucket_range, const void* key, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(httd_p == NULL || pam_p == NULL)
		return NULL;

	// if key is absent, then the bucket_range should be valid
	if(key == NULL && !is_valid_page_table_bucket_range(&bucket_range))
		return NULL;

	hash_table_iterator* hti_p = malloc(sizeof(hash_table_iterator));
	if(hti_p == NULL)
		exit(-1);

	hti_p->root_page_id = root_page_id;
	hti_p->key = key;
	hti_p->bucket_range = bucket_range;
	hti_p->httd_p = httd_p;
	hti_p->pam_p = pam_p;
	hti_p->pmm_p = pmm_p;

	hti_p->ptrl_p = NULL;
	hti_p->lpli_p = NULL;

	// fetch the bucket_count of the hash_table
	// take a range lock on the page table, to get the bucket_count
	hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count = UINT64_MAX;
	find_non_NULL_PAGE_ID_in_page_table(hti_p->ptrl_p, &bucket_count, LESSER_THAN_EQUALS, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_EVERYTHING_AND_ABORT;

	if(hti_p->key != NULL)
	{
		// get the bucket for the key
		hti_p->curr_bucket_id = get_bucket_index_for_key_using_hash_table_tuple_definitions(hti_p->httd_p, hti_p->key, bucket_count);

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
		// limit bucket_range to subset [0, bucket_count-1], discarding the buckets that come after bucket_count buckets
		if(hti_p->bucket_range.first_bucket_id >= bucket_count)
			goto DELETE_EVERYTHING_AND_ABORT;
		hti_p->bucket_range.last_bucket_id = max(hti_p->bucket_range.last_bucket_id, bucket_count - 1);

		// make curr_bucket_id point to the first bucket in the range
		hti_p->curr_bucket_id = hti_p->bucket_range.first_bucket_id;

		// minimize the lock range to the preferred one
		minimize_lock_range_for_page_table_range_locker(hti_p->ptrl_p, hti_p->bucket_range, transaction_id, abort_error);
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

int is_writable_hash_table_iterator(const hash_table_iterator* hti_p)
{
	return hti_p->pmm_p != NULL;
}

const void* get_tuple_hash_table_iterator(const hash_table_iterator* hti_p)
{
	// TODO
}

int next_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error)
{
	// TODO
}

int prev_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error)
{
	// TODO
}

int insert_in_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error)
{
	// TODO
}

int update_at_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error)
{
	// TODO
}

int remove_from_hash_table_iterator(hash_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int update_non_key_element_in_place_at_hash_table_iterator(hash_table_iterator* hti_p, uint32_t element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
{
	// TODO
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
		hti_p->ptrl_p = get_new_page_table_range_locker(hti_p->root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(hti_p->httd_p->pttd), hti_p->pam_p, hti_p->pmm_p, transaction_id, abort_error);
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

			// if the corresponding linked_page_list exists and is empty, then delete it and set it's entry in page_table as NULL
			if(is_empty_linked_page_list(hti_p->lpli_p))
			{
				// release lock on hti_p->lpli_p
				delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
				hti_p->lpli_p = NULL;
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;

				// destroy the linked_page_list
				destroy_linked_page_list(curr_bucket_head_page_id, &(hti_p->httd_p->lpltd), hti_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;

				set_in_page_table(hti_p->ptrl_p, hti_p->curr_bucket_id, hti_p->httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
				if(*abort_error)
					goto DELETE_EVERYTHING_AND_ABORT;
			}
			else
			{
				// release lock on hti_p->lpli_p
				delete_linked_page_list_iterator(hti_p->lpli_p, transaction_id, abort_error);
				hti_p->lpli_p = NULL;
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