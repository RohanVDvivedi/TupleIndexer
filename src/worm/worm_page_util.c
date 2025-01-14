#include<worm_page_util.h>

int init_worm_head_page(persistent_page* ppage, uint32_t reference_count, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int init_worm_any_page(persistent_page* ppage, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

uint32_t blob_bytes_insertable_for_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	// TODO
}