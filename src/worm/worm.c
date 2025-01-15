#include<worm.h>

uint64_t get_new_worm(uint64_t reference_counter, uint64_t dependent_root, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int increment_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int decrement_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

void print_worm(uint64_t root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// TODO
}