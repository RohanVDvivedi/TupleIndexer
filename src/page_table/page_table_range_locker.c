#include<page_table_range_locker.h>

#include<persistent_page_functions.h>
#include<locked_pages_stack.h>
#include<invalid_tuple_indices.h>

#include<stdlib.h>

page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, bucket_range lock_range, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return (page_table_range_locker*)get_new_array_table_range_locker(root_page_id, lock_range, &(pttd_p->attd), pam_p, pmm_p, transaction_id, abort_error);
}

int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, bucket_range lock_range, const void* transaction_id, int* abort_error)
{
	return minimize_lock_range_for_array_table_range_locker(&(ptrl_p->atrl), lock_range, transaction_id, abort_error);
}

bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p)
{
	return get_lock_range_for_array_table_range_locker(&(ptrl_p->atrl));
}

int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p)
{
	return is_writable_array_table_range_locker(&(ptrl_p->atrl));
}

uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, const void* transaction_id, int* abort_error)
{
	// TODO
}

int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const void* transaction_id, int* abort_error)
{
	// TODO
}

uint64_t find_non_NULL_PAGE_ID_in_page_table(page_table_range_locker* ptrl_p, uint64_t* bucket_id, find_position find_pos, const void* transaction_id, int* abort_error)
{
	// TODO
}

void delete_page_table_range_locker(page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error)
{
	return delete_array_table_range_locker(&(ptrl_p->atrl), transaction_id, abort_error);
}