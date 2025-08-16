#include<tupleindexer/heap_table/heap_table.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>

uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_bplus_tree(&(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

void fix_unused_space_in_heap_table(uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void track_unused_space_in_heap_table(const persistent_page* ppage, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);