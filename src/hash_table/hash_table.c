#include<hash_table.h>

#include<hash_table_tuple_definitions.h>

#include<page_table.h>
#include<linked_page_list.h>

uint64_t get_new_hash_table(uint64_t initial_bucket_count, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// initial_bucket_count can not be 0
	// it has to be 1 at minimum
	initial_bucket_count = max(1, initial_bucket_count);

	// create a new page_table for the hash_table
	uint64_t root_page_id = get_new_page_table(&(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return httd_p->pttd.pas_p->NULL_PAGE_ID;

	// take a range lock on the page table, to set the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, (bucket_range){initial_bucket_count, initial_bucket_count}, &(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return httd_p->pttd.pas_p->NULL_PAGE_ID;

	// set the initial_bucket_count
	set_in_page_table(ptrl_p, initial_bucket_count, root_page_id, transaction_id, abort_error);
	if(*abort_error)
	{
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
		return httd_p->pttd.pas_p->NULL_PAGE_ID;
	}

	delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(*abort_error)
		return httd_p->pttd.pas_p->NULL_PAGE_ID;

	return root_page_id;
}

uint64_t get_bucket_count_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// take a range lock on the page table
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, MAX, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	//EXIT:; // -> unused label, i.e. for the happy case
	ABORT_ERROR:;
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;
	return bucket_count;
}

typedef struct hash_table_bucket hash_table_bucket;
struct hash_table_bucket
{
	uint64_t bucket_id;								// index of the bucket in the page_table of the hash_table
	uint64_t bucket_head_page_id;					// head_page_id of the bucket at bucket_index
	linked_page_list_iterator* bucket_iterator;		// defaults to NULL, unless the iterator is not initialized
};

int expand_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// zero initialize all local variables of the function
	int result = 0;
	page_table_range_locker* ptrl_p = NULL;
	linked_page_list_iterator* split_content = NULL; uint64_t split_content_head_page_id = 0;
	hash_table_bucket split_hash_buckets[2] = {};

	// take a range lock on the page table
	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, MAX, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// if bucket_count == UINT64_MAX, then fail
	if(bucket_count == UINT64_MAX)
	{
		result = 0;
		goto EXIT;
	}

	// page_table[bucket_count] = NULL
	set_in_page_table(ptrl_p, bucket_count, httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// get both the bucket ids that we would be splitting into
	int64_t temp;
	split_hash_buckets[0].bucket_id = get_hash_table_split_index(bucket_count, &temp);
	split_hash_buckets[1].bucket_id = bucket_count;

	// page_table[++bucket_count] = root_page_id
	set_in_page_table(ptrl_p, ++bucket_count, root_page_id, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// initialize split_content_head_page_id
	split_content_head_page_id = get_from_page_table(ptrl_p, split_hash_buckets[0].bucket_id, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// set both the page_table buckets to NULL

	// page_table[split_hash_buckets[0].bucket_id] = httd_p->pttd.pas_p->NULL_PAGE_ID
	set_in_page_table(ptrl_p, split_hash_buckets[0].bucket_id, httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// page_table[split_hash_buckets[1].bucket_id] = httd_p->pttd.pas_p->NULL_PAGE_ID
	set_in_page_table(ptrl_p, split_hash_buckets[1].bucket_id, httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// now we only need to work with split_hash_buckets [0] and [1]
	minimize_lock_range_for_page_table_range_locker(ptrl_p, (bucket_range){split_hash_buckets[0].bucket_id, split_hash_buckets[1].bucket_id}, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	if(split_content_head_page_id == httd_p->pttd.pas_p->NULL_PAGE_ID)
	{
		result = 1;
		goto EXIT;
	}

	// open a linked_page_list iterator for the split_content_head_page_id
	split_content = get_new_linked_page_list_iterator(split_content_head_page_id, &(httd_p->lpltd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// iterate while split_content still has tuples
	while(!is_empty_linked_page_list(split_content))
	{
		// fetch the next tuple to be processed
		const void* record_tuple = get_tuple_linked_page_list_iterator(split_content);

		if(record_tuple == NULL)
		{
			// discard the processed tuple from split_content
			remove_from_linked_page_list_iterator(split_content, GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			continue;
		}

		// calculate the bucket_id for the record_tuple
		uint64_t bucket_id = get_bucket_index_for_record_using_hash_table_tuple_definitions(httd_p, record_tuple, bucket_count);

		// loop over to find the right bucket to insert into
		// we are here doing a linear search, and since there are only 2 elements to search from, it will never be a performance bottleneck
		for(int i = 0; i < sizeof(split_hash_buckets)/sizeof(split_hash_buckets[0]); i++)
		{
			// if it is not the right bucket, then continue
			if(split_hash_buckets[i].bucket_id != bucket_id)
				continue;

			// there is quite a bit of work that needs to be done, if the corresponding iterator is NULL
			if(split_hash_buckets[i].bucket_iterator == NULL)
			{
				// create a new linked_page_list bucket
				split_hash_buckets[i].bucket_head_page_id = get_new_linked_page_list(&(httd_p->lpltd), pam_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// page_table[split_hash_buckets[i].bucket_id] = split_hash_buckets[i].bucket_head_page_id
				set_in_page_table(ptrl_p, split_hash_buckets[i].bucket_id, split_hash_buckets[i].bucket_head_page_id, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// open a new bucket iterator for the ith split_hash_bucket
				split_hash_buckets[i].bucket_iterator = get_new_linked_page_list_iterator(split_hash_buckets[i].bucket_head_page_id, &(httd_p->lpltd), pam_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// now based on the currently open iterators of the split_hash_buckets, we shrink or completely destroy the ptrl iterator
				bucket_range new_range = {UINT64_MAX, 0};
				for(int j = 0; j < sizeof(split_hash_buckets)/sizeof(split_hash_buckets[0]); j++)
				{
					if(split_hash_buckets[j].bucket_iterator != NULL)
						continue;

					new_range.first_bucket_id = min(new_range.first_bucket_id, split_hash_buckets[j].bucket_id);
					new_range.last_bucket_id = max(new_range.last_bucket_id, split_hash_buckets[j].bucket_id);
				}

				if(new_range.first_bucket_id > new_range.last_bucket_id)
				{
					delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
					ptrl_p = NULL;
					if(*abort_error)
						goto ABORT_ERROR;
				}
				else
				{
					minimize_lock_range_for_page_table_range_locker(ptrl_p, new_range, transaction_id, abort_error);
					if(*abort_error)
						goto ABORT_ERROR;
				}
			}

			// insert the tuple into the bucket_iterator and break out of this loop
			int inserted = insert_at_linked_page_list_iterator(split_hash_buckets[i].bucket_iterator, record_tuple, INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// it would almost always succeed
			if(inserted)
			{
				// once insert to before of a head has succeeded, we go prevous by 1 tuple to again point to head, this way we are always at head
				// it is assumed that if you always point the linked_page_list_iterator to head, then there are lesser pages you need an active lock on hence this optimization
				prev_linked_page_list_iterator(split_hash_buckets[i].bucket_iterator, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}

			break;
		}

		// discard the processed tuple from split_content
		remove_from_linked_page_list_iterator(split_content, GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// now we can delete the iterator
	delete_linked_page_list_iterator(split_content, transaction_id, abort_error);
	split_content = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	// now we can destroy the linked_page_list corresponding to the split_content
	destroy_linked_page_list(split_content_head_page_id, &(httd_p->lpltd), pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// now we can clean up any remaining ressources including the ptrl_p, and the split_hash_buckets[0 and 1].bucket_iterator-s

	result = 1;
	goto EXIT;

	EXIT:;
	ABORT_ERROR:;
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(split_content != NULL)
		delete_linked_page_list_iterator(split_content, transaction_id, abort_error);
	if(split_hash_buckets[0].bucket_iterator != NULL)
		delete_linked_page_list_iterator(split_hash_buckets[0].bucket_iterator, transaction_id, abort_error);
	if(split_hash_buckets[1].bucket_iterator != NULL)
		delete_linked_page_list_iterator(split_hash_buckets[1].bucket_iterator, transaction_id, abort_error);
	if(*abort_error)
		return 0;
	return result;
}

int shrink_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int result = 0;

	// take a range lock on the page table
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, MAX, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// if bucket_count == 1, then fail
	if(bucket_count == 1)
	{
		result = 0;
		goto EXIT;
	}

	// page_table[bucket_count] = NULL
	set_in_page_table(ptrl_p, bucket_count, httd_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// fetch the bucket_id -> head_bucket_id that needs to be merged
	uint64_t merge_bucket_head_page_id = get_from_page_table(ptrl_p, bucket_count - 1, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// decrement the bucket_count
	// page_table[--bucket_count] = NULL
	set_in_page_table(ptrl_p, --bucket_count, root_page_id, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// if this merge_bucket_head_id is NULL, then we are done
	if(merge_bucket_head_page_id == httd_p->pttd.pas_p->NULL_PAGE_ID)
	{
		result = 1;
		goto EXIT;
	}

	// fetch the split_bucket_head_page_id
	int64_t temp;
	uint64_t split_index = get_hash_table_split_index(bucket_count, &temp);
	uint64_t split_bucket_head_page_id = get_from_page_table(ptrl_p, split_index, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	if(split_bucket_head_page_id == httd_p->pttd.pas_p->NULL_PAGE_ID)
	{
		// bucket at split_index is NULL, so set it to merge_bucket_head_page_id
		set_in_page_table(ptrl_p, split_index, merge_bucket_head_page_id, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}
	else
	{
		// both of them are not NULL, so perform an actual merge
		merge_linked_page_lists(split_bucket_head_page_id, merge_bucket_head_page_id, &(httd_p->lpltd), pam_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	result = 1;
	goto EXIT;

	EXIT:;
	ABORT_ERROR:;
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;
	return result;
}

int destroy_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// take a range lock on the page table, to get the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, MAX, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;

	// iterate over all the buckets and destroy them, without altering the page_table
	uint64_t bucket_id = 0;
	uint64_t bucket_head_page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN_EQUALS, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;

	while(bucket_id < bucket_count && bucket_head_page_id != httd_p->pttd.pas_p->NULL_PAGE_ID)
	{
		// destroy the linked_page_list at the bucket_head_page_id
		destroy_linked_page_list(bucket_head_page_id, &(httd_p->lpltd), pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;

		bucket_head_page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;
	}

	delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// now you may destroy the page_table
	destroy_page_table(root_page_id, &(httd_p->pttd), pam_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;

	DELETE_BUCKET_RANGE_LOCKER_AND_ABORT:;
	delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	return 0;
}

void print_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// take a range lock on the page table, to get the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
		return ;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, MAX, transaction_id, abort_error);
	if(*abort_error)
		goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;

	// print the root page id of the hash_table, and it's bucket_count
	printf("\n\nHash_table @ root_page_id = %"PRIu64", and bucket_count = %"PRIu64"\n\n", root_page_id, bucket_count);

	// iterate over all the buckets, printing their contents
	for(uint64_t bucket_id = 0; bucket_id < bucket_count; bucket_id++)
	{
		uint64_t bucket_head_page_id = get_from_page_table(ptrl_p, bucket_id, transaction_id, abort_error);
		if(*abort_error)
			goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;

		if(bucket_head_page_id != httd_p->pttd.pas_p->NULL_PAGE_ID)
		{
			printf("%"PRIu64" -> %"PRIu64"\n\n", bucket_id, bucket_head_page_id);
			// destroy the linked_page_list at the bucket_head_page_id
			print_linked_page_list(bucket_head_page_id, &(httd_p->lpltd), pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto DELETE_BUCKET_RANGE_LOCKER_AND_ABORT;
		}
		else
			printf("%"PRIu64" -> EMPTY_BUCKET\n\n", bucket_id);
	}

	delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	if(*abort_error)
		return ;

	return ;

	DELETE_BUCKET_RANGE_LOCKER_AND_ABORT:;
	delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	return ;
}