#include<hash_table_iterator.h>

hash_table_iterator* get_new_hash_table_iterator(uint64_t root_page_id, page_table_bucket_range bucket_range, const void* key, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
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
	// TODO
}