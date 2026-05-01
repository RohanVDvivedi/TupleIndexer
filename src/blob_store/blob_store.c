#include<tupleindexer/blob_store/blob_store.h>

#include<tupleindexer/heap_table/heap_table.h>

uint64_t get_new_blob_store(const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_heap_table(&(bstd_p->httd), pam_p, pmm_p, transaction_id, abort_error);
}

int destroy_blob_store(uint64_t root_page_id, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	return destroy_heap_table(root_page_id, &(bstd_p->httd), pam_p, transaction_id, abort_error);
}

uint32_t get_root_level_blob_store(uint64_t root_page_id, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	return get_root_level_heap_table(root_page_id, &(bstd_p->httd), pam_p, transaction_id, abort_error) + 1;
}