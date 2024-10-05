#include<page_table_range_locker.h>

#include<persistent_page_functions.h>
#include<locked_pages_stack.h>
#include<invalid_tuple_indices.h>
#include<tuple_for_page_id.h>

#include<stdlib.h>

page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, bucket_range lock_range, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return (page_table_range_locker*)get_new_array_table_range_locker(root_page_id, lock_range, &(pttd_p->attd), pam_p, pmm_p, transaction_id, abort_error);
}

page_table_range_locker* clone_page_table_range_locker(const page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error)
{
	return (page_table_range_locker*)clone_array_table_range_locker(&(ptrl_p->atrl), transaction_id, abort_error);
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
	char page_id_entry_memory[MAX_TUPLE_SIZE_FOR_ONLY_NON_NULLABLE_FIXED_WIDTH_UNSIGNED_PAGE_ID];

	const void* res = get_from_array_table(&(ptrl_p->atrl), bucket_id, page_id_entry_memory, transaction_id, abort_error);
	if(*abort_error)
		return ptrl_p->atrl.attd_p->pas_p->NULL_PAGE_ID;

	if(res == NULL)
		return ptrl_p->atrl.attd_p->pas_p->NULL_PAGE_ID;
	return get_value_from_element_from_tuple(ptrl_p->atrl.attd_p->record_def, SELF, res).uint_value;
}

int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const void* transaction_id, int* abort_error)
{
	char page_id_entry_memory[MAX_TUPLE_SIZE_FOR_ONLY_NON_NULLABLE_FIXED_WIDTH_UNSIGNED_PAGE_ID];

	void* page_id_entry = NULL;
	if(page_id != ptrl_p->atrl.attd_p->pas_p->NULL_PAGE_ID)
	{
		page_id_entry = page_id_entry_memory;
		init_tuple(ptrl_p->atrl.attd_p->record_def, page_id_entry);
		set_element_in_tuple(ptrl_p->atrl.attd_p->record_def, SELF, page_id_entry, &((user_value){.uint_value = page_id}), UINT32_MAX);
	}

	return set_in_array_table(&(ptrl_p->atrl), bucket_id, page_id_entry, transaction_id, abort_error);
}

uint64_t find_non_NULL_PAGE_ID_in_page_table(page_table_range_locker* ptrl_p, uint64_t* bucket_id, find_position find_pos, const void* transaction_id, int* abort_error)
{
	char page_id_entry_memory[MAX_TUPLE_SIZE_FOR_ONLY_NON_NULLABLE_FIXED_WIDTH_UNSIGNED_PAGE_ID];

	const void* res = find_non_NULL_entry_in_array_table(&(ptrl_p->atrl), bucket_id, page_id_entry_memory, find_pos, transaction_id, abort_error);
	if(*abort_error)
		return ptrl_p->atrl.attd_p->pas_p->NULL_PAGE_ID;

	if(res == NULL)
		return ptrl_p->atrl.attd_p->pas_p->NULL_PAGE_ID;
	return get_value_from_element_from_tuple(ptrl_p->atrl.attd_p->record_def, SELF, res).uint_value;
}

void delete_page_table_range_locker(page_table_range_locker* ptrl_p, uint64_t* vaccum_bucket_id, int* vaccum_needed, const void* transaction_id, int* abort_error)
{
	return delete_array_table_range_locker(&(ptrl_p->atrl), vaccum_bucket_id, vaccum_needed, transaction_id, abort_error);
}

int perform_vaccum_page_table_range_locker(page_table_range_locker* ptrl_p, uint64_t vaccum_bucket_id, const void* transaction_id, int* abort_error)
{
	return perform_vaccum_array_table_range_locker(&(ptrl_p->atrl), vaccum_bucket_id, transaction_id, abort_error);
}