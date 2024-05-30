#include<hash_table.h>

#include<page_table.h>

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