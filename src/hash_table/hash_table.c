#include<hash_table.h>

#include<page_table.h>
#include<linked_page_list.h>

uint64_t get_new_hash_table(uint64_t initial_bucket_count, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// create a new page_table for the hash_table
	uint64_t root_page_id = get_new_page_table(&(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return httd_p->pttd.pas_p->NULL_PAGE_ID;

	// take a range lock on the page table, to set the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, (page_table_bucket_range){initial_bucket_count, initial_bucket_count}, &(httd_p->pttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return httd_p->pttd.pas_p->NULL_PAGE_ID;

	// set the initial_bucket_count
	set_in_page_table(ptrl_p, initial_bucket_count, initial_bucket_count, transaction_id, abort_error);
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

int expand_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int shrink_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int resize_hash_table(uint64_t root_page_id, uint64_t new_bucket_count, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int destroy_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// take a range lock on the page table, to get the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count = UINT64_MAX;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, LESSER_THAN_EQUALS, transaction_id, abort_error);
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

void print_hash_table(uint64_t root_page_id, int only_buckets, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// take a range lock on the page table, to get the bucket_count
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(httd_p->pttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
		return ;

	// get the current bucket_count of the hash_table
	uint64_t bucket_count = UINT64_MAX;
	find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_count, LESSER_THAN_EQUALS, transaction_id, abort_error);
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